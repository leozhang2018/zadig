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

package models

import "go.mongodb.org/mongo-driver/bson/primitive"

const (
	TerminalSessionStatusActive = "active"
	TerminalSessionStatusClosed = "closed"

	TerminalSessionTypeEnvironment = "env"
	TerminalSessionTypeWorkflow    = "workflow"
)

type TerminalSessionRecord struct {
	ID           primitive.ObjectID `bson:"_id,omitempty"          json:"id"`
	SessionID    string             `bson:"session_id"             json:"session_id"`
	UserName     string             `bson:"username"               json:"username"`
	ProjectName  string             `bson:"project_name"           json:"project_name"`
	ServiceName  string             `bson:"service_name"           json:"service_name"`
	PodName      string             `bson:"pod_name"               json:"pod_name"`
	Container    string             `bson:"container"              json:"container"`
	Namespace    string             `bson:"namespace"              json:"namespace"`
	ClusterID    string             `bson:"cluster_id"             json:"cluster_id"`
	Type         string             `bson:"type"                   json:"type"`      // env / workflow
	Status       string             `bson:"status"                 json:"status"`    // active / closed
	StartTime    int64              `bson:"start_time"             json:"start_time"`
	EndTime      int64              `bson:"end_time"               json:"end_time"`
	CastData     []byte             `bson:"cast_data,omitempty"    json:"-"`         // gzip 压缩的 asciicast v2
	CastSize     int64              `bson:"cast_size"              json:"cast_size"` // 未压缩原始大小（字节）
	Truncated    bool               `bson:"truncated"              json:"truncated"` // 是否因超限被截断
	SearchContent string            `bson:"search_content,omitempty" json:"-"`       // 去 ANSI 码纯文本，用于 $text 全文检索（上限 2MB）
	WorkflowName string             `bson:"workflow_name,omitempty" json:"workflow_name,omitempty"`
	TaskID       int64              `bson:"task_id,omitempty"      json:"task_id,omitempty"`
}

func (TerminalSessionRecord) TableName() string {
	return "terminal_session_record"
}
