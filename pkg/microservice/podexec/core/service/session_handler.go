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

package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
	commonrepo "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/mongodb"
	internalhandler "github.com/koderover/zadig/v2/pkg/shared/handler"
	e "github.com/koderover/zadig/v2/pkg/tool/errors"
	"github.com/koderover/zadig/v2/pkg/tool/log"
)

// watchUpgrader allows any origin for the watch WebSocket endpoint.
var watchUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// ListActiveSessions returns in-memory active sessions with optional filters and pagination.
//
// GET /api/podexec/sessions?username=&project_name=&pod_name=&page=&page_size=
func ListActiveSessions(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	if err != nil {
		ctx.RespErr = e.ErrUnauthorized.AddErr(err)
		ctx.UnAuthorized = true
		return
	}

	username := c.Query("username")
	projectName := c.Query("project_name")
	podName := c.Query("pod_name")

	page := 1
	pageSize := 20
	if v := c.Query("page"); v != "" {
		if p, err2 := strconv.Atoi(v); err2 == nil && p > 0 {
			page = p
		}
	}
	if v := c.Query("page_size"); v != "" {
		if ps, err2 := strconv.Atoi(v); err2 == nil && ps > 0 {
			if ps > 100 {
				ps = 100
			}
			pageSize = ps
		}
	}

	type sessionItem struct {
		SessionID   string `json:"session_id"`
		UserName    string `json:"username"`
		ProjectName string `json:"project_name"`
		ServiceName string `json:"service_name"`
		PodName     string `json:"pod_name"`
		Container   string `json:"container"`
		Type        string `json:"type"`
		StartTime   int64  `json:"start_time"`
	}

	all := globalSessionManager.ListActive()

	// filter
	var filtered []sessionItem
	for _, r := range all {
		if username != "" && r.UserName != username {
			continue
		}
		if projectName != "" && r.ProjectName != projectName {
			continue
		}
		if podName != "" && r.PodName != podName {
			continue
		}
		filtered = append(filtered, sessionItem{
			SessionID:   r.SessionID,
			UserName:    r.UserName,
			ProjectName: r.ProjectName,
			ServiceName: r.ServiceName,
			PodName:     r.PodName,
			Container:   r.Container,
			Type:        r.Type,
			StartTime:   r.StartTime,
		})
	}

	total := int64(len(filtered))

	// paginate
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > len(filtered) {
		start = len(filtered)
	}
	if end > len(filtered) {
		end = len(filtered)
	}
	paged := filtered[start:end]
	if paged == nil {
		paged = []sessionItem{}
	}

	type listResp struct {
		Total   int64         `json:"total"`
		Records []sessionItem `json:"records"`
	}
	ctx.Resp = listResp{Total: total, Records: paged}
}

// ListHistorySessions returns paginated historical sessions from MongoDB.
//
// GET /api/podexec/sessions/history?username=&project_name=&service_name=&pod_name=
//
//	&start_time_from=&start_time_to=&keyword=&page=&page_size=
func ListHistorySessions(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	if err != nil {
		ctx.RespErr = e.ErrUnauthorized.AddErr(err)
		ctx.UnAuthorized = true
		return
	}

	opt := &commonrepo.ListTerminalSessionOption{
		Status:      "closed",
		UserName:    c.Query("username"),
		ProjectName: c.Query("project_name"),
		ServiceName: c.Query("service_name"),
		PodName:     c.Query("pod_name"),
		Keyword:     c.Query("keyword"),
	}

	if v := c.Query("start_time_from"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			opt.StartTimeFrom = ts
		}
	}
	if v := c.Query("start_time_to"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			opt.StartTimeTo = ts
		}
	}
	if v := c.Query("page"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			opt.Page = p
		}
	}
	if v := c.Query("page_size"); v != "" {
		if ps, err := strconv.Atoi(v); err == nil {
			opt.PageSize = ps
		}
	}

	records, total, err := commonrepo.NewTerminalSessionColl().List(context.Background(), opt)
	if err != nil {
		ctx.RespErr = e.ErrInternalError.AddErr(err)
		return
	}

	if records == nil {
		records = []*commonmodels.TerminalSessionRecord{}
	}
	type listResp struct {
		Total   int64                                    `json:"total"`
		Records []*commonmodels.TerminalSessionRecord   `json:"records"`
	}
	ctx.Resp = listResp{Total: total, Records: records}
}

// InterruptSession forcefully terminates a running session.
//
// DELETE /api/podexec/sessions/:sessionID
func InterruptSession(c *gin.Context) {
	ctx, err := internalhandler.NewContextWithAuthorization(c)
	defer func() { internalhandler.JSONResponse(c, ctx) }()
	if err != nil {
		ctx.RespErr = e.ErrUnauthorized.AddErr(err)
		ctx.UnAuthorized = true
		return
	}

	sessionID := c.Param("sessionID")
	if sessionID == "" {
		ctx.RespErr = e.ErrInvalidParam.AddDesc("sessionID is required")
		return
	}

	if err := globalSessionManager.Interrupt(sessionID); err != nil {
		ctx.RespErr = e.ErrInternalError.AddErr(err)
		return
	}
}

// GetSessionCast returns the asciicast v2 file for a historical session.
// The response is plain text (gzip-decompressed) suitable for asciinema-player.
//
// GET /api/podexec/sessions/:sessionID/cast
func GetSessionCast(c *gin.Context) {
	sessionID := c.Param("sessionID")
	if sessionID == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	castData, err := commonrepo.NewTerminalSessionColl().FindCastData(context.Background(), sessionID)
	if err != nil {
		log.Errorf("FindCastData(%s) err: %v", sessionID, err)
		c.Status(http.StatusNotFound)
		return
	}
	if len(castData) == 0 {
		c.Status(http.StatusNoContent)
		return
	}

	gr, err := gzip.NewReader(bytes.NewReader(castData))
	if err != nil {
		log.Errorf("gzip.NewReader(%s) err: %v", sessionID, err)
		c.Status(http.StatusInternalServerError)
		return
	}
	defer gr.Close()

	c.Header("Content-Type", "text/plain; charset=utf-8")
	c.Header("Cache-Control", "no-store")
	c.Status(http.StatusOK)

	buf := make([]byte, 32*1024)
	for {
		n, readErr := gr.Read(buf)
		if n > 0 {
			if _, writeErr := c.Writer.Write(buf[:n]); writeErr != nil {
				return
			}
		}
		if readErr != nil {
			break
		}
	}
}

// WatchSession streams live stdout from a running session over WebSocket.
// On connect, the full snapshot (fast-forward) is sent first, then real-time output.
//
// GET /api/podexec/sessions/:sessionID/watch  (WebSocket upgrade)
func WatchSession(c *gin.Context) {
	sessionID := c.Param("sessionID")
	if sessionID == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	recorder, ok := globalSessionManager.GetRecorder(sessionID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "session not found or already closed"})
		return
	}

	conn, err := watchUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Errorf("WatchSession upgrade err: %v", err)
		return
	}
	defer conn.Close()

	// Send the historical snapshot so the watcher is caught up.
	if snapshot := recorder.Snapshot(); len(snapshot) > 0 {
		if err := conn.WriteMessage(websocket.BinaryMessage, snapshot); err != nil {
			return
		}
	}

	// Subscribe to live output.
	ch := recorder.Subscribe()
	defer recorder.Unsubscribe(ch)

	// Forward live events; also handle watcher disconnect via a read goroutine.
	closeCh := make(chan struct{})
	go func() {
		defer close(closeCh)
		for {
			// Drain incoming frames (no-op for read-only, but detects disconnect).
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case data, more := <-ch:
			if !more {
				// Recorder closed the channel (session ended).
				return
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				return
			}
		case <-closeCh:
			return
		}
	}
}
