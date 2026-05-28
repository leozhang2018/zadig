/*
Copyright 2025 The KodeRover Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mongodb

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/koderover/zadig/v2/pkg/microservice/aslan/config"
	"github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	mongotool "github.com/koderover/zadig/v2/pkg/tool/mongo"
)

type TerminalSessionColl struct {
	*mongo.Collection
	mongo.Session

	coll string
}

func NewTerminalSessionColl() *TerminalSessionColl {
	name := models.TerminalSessionRecord{}.TableName()
	return &TerminalSessionColl{
		Collection: mongotool.Database(config.MongoDatabase()).Collection(name),
		coll:       name,
	}
}

func (c *TerminalSessionColl) GetCollectionName() string {
	return c.coll
}

func (c *TerminalSessionColl) EnsureIndex(ctx context.Context) error {
	mod := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "session_id", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("idx_session_id"),
		},
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "start_time", Value: -1},
			},
			Options: options.Index().SetName("idx_status_start_time"),
		},
		{
			Keys:    bson.D{{Key: "search_content", Value: "text"}},
			Options: options.Index().SetName("idx_search_content_text"),
		},
	}
	_, err := c.Indexes().CreateMany(ctx, mod, mongotool.CreateIndexOptions(ctx))
	return err
}

func (c *TerminalSessionColl) Create(ctx context.Context, record *models.TerminalSessionRecord) error {
	if record == nil {
		return errors.New("terminal session record is nil")
	}
	if record.StartTime == 0 {
		record.StartTime = time.Now().UnixMilli()
	}
	_, err := c.InsertOne(ctx, record)
	return err
}

// UpdateClosed finalizes a session: sets status=closed, end_time, cast data, and search content.
func (c *TerminalSessionColl) UpdateClosed(ctx context.Context, sessionID string, endTime int64, castData []byte, castSize int64, searchContent string, truncated bool) error {
	filter := bson.M{"session_id": sessionID}
	update := bson.M{"$set": bson.M{
		"status":         models.TerminalSessionStatusClosed,
		"end_time":       endTime,
		"cast_data":      castData,
		"cast_size":      castSize,
		"search_content": searchContent,
		"truncated":      truncated,
	}}
	_, err := c.UpdateOne(ctx, filter, update)
	return err
}

// FindBySessionID returns the full record (excluding cast_data) for a given sessionID.
func (c *TerminalSessionColl) FindBySessionID(ctx context.Context, sessionID string) (*models.TerminalSessionRecord, error) {
	filter := bson.M{"session_id": sessionID}
	proj := options.FindOne().SetProjection(bson.M{"cast_data": 0, "search_content": 0})

	var record models.TerminalSessionRecord
	if err := c.FindOne(ctx, filter, proj).Decode(&record); err != nil {
		return nil, err
	}
	return &record, nil
}

// FindCastData returns only the cast_data field for a session.
func (c *TerminalSessionColl) FindCastData(ctx context.Context, sessionID string) ([]byte, error) {
	filter := bson.M{"session_id": sessionID}
	proj := options.FindOne().SetProjection(bson.M{"cast_data": 1})

	var record models.TerminalSessionRecord
	if err := c.FindOne(ctx, filter, proj).Decode(&record); err != nil {
		return nil, err
	}
	return record.CastData, nil
}

type ListTerminalSessionOption struct {
	Status        string // "active" / "closed" / "" (all)
	UserName      string
	ProjectName   string
	ServiceName   string
	PodName       string
	StartTimeFrom int64 // unix ms, inclusive
	StartTimeTo   int64 // unix ms, inclusive
	Keyword       string // $text search
	Page          int
	PageSize      int
}

// List returns paginated terminal session records (without cast_data and search_content).
func (c *TerminalSessionColl) List(ctx context.Context, opt *ListTerminalSessionOption) ([]*models.TerminalSessionRecord, int64, error) {
	if opt == nil {
		return nil, 0, errors.New("nil ListTerminalSessionOption")
	}

	filter := bson.M{}
	if opt.Status != "" {
		filter["status"] = opt.Status
	}
	if opt.UserName != "" {
		filter["username"] = opt.UserName
	}
	if opt.ProjectName != "" {
		filter["project_name"] = opt.ProjectName
	}
	if opt.ServiceName != "" {
		filter["service_name"] = opt.ServiceName
	}
	if opt.PodName != "" {
		filter["pod_name"] = opt.PodName
	}
	if opt.StartTimeFrom > 0 || opt.StartTimeTo > 0 {
		timeRange := bson.M{}
		if opt.StartTimeFrom > 0 {
			timeRange["$gte"] = opt.StartTimeFrom
		}
		if opt.StartTimeTo > 0 {
			timeRange["$lte"] = opt.StartTimeTo
		}
		filter["start_time"] = timeRange
	}
	if opt.Keyword != "" {
		// Try $text first; fall back to regex if the text index does not exist.
		textFilter := bson.M{"$text": bson.M{"$search": opt.Keyword}}
		for k, v := range textFilter {
			filter[k] = v
		}
	}

	total, err := c.Collection.CountDocuments(mongotool.SessionContext(ctx, c.Session), filter)
	if err != nil {
		if opt.Keyword != "" && isTextIndexNotFound(err) {
			// Text index missing: fall back to case-insensitive regex on search_content.
			delete(filter, "$text")
			filter["search_content"] = primitive.Regex{
				Pattern: strings.ReplaceAll(opt.Keyword, " ", "|"),
				Options: "i",
			}
			total, err = c.Collection.CountDocuments(mongotool.SessionContext(ctx, c.Session), filter)
		}
		if err != nil {
			return nil, 0, err
		}
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "start_time", Value: -1}}).
		SetProjection(bson.M{"cast_data": 0, "search_content": 0})

	page := opt.Page
	pageSize := opt.PageSize
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	findOpts.SetSkip(int64((page - 1) * pageSize)).SetLimit(int64(pageSize))

	cursor, err := c.Collection.Find(mongotool.SessionContext(ctx, c.Session), filter, findOpts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	var records []*models.TerminalSessionRecord
	if err := cursor.All(mongotool.SessionContext(ctx, c.Session), &records); err != nil {
		return nil, 0, err
	}

	return records, total, nil
}

// isTextIndexNotFound returns true when the error indicates a missing $text index.
func isTextIndexNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "text index required") ||
		strings.Contains(msg, "IndexNotFound")
}
