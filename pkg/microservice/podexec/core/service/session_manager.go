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
	"context"
	"fmt"
	"sync"

	commonmodels "github.com/koderover/zadig/v2/pkg/microservice/aslan/core/common/repository/models"
)

// ActiveSession represents a currently running terminal session held in memory.
type ActiveSession struct {
	Record   *commonmodels.TerminalSessionRecord
	Recorder *AsciicastRecorder
	cancel   context.CancelFunc
}

// SessionManager is an in-memory store of all active terminal sessions.
// It is safe for concurrent use.
type SessionManager struct {
	sessions sync.Map // key: sessionID (string) → *ActiveSession
}

// globalSessionManager is the process-wide singleton.
var globalSessionManager = &SessionManager{}

// Register stores a new active session.
func (m *SessionManager) Register(s *ActiveSession) {
	m.sessions.Store(s.Record.SessionID, s)
}

// Unregister removes and returns the active session, or nil if not found.
func (m *SessionManager) Unregister(sessionID string) *ActiveSession {
	v, loaded := m.sessions.LoadAndDelete(sessionID)
	if !loaded {
		return nil
	}
	return v.(*ActiveSession)
}

// Interrupt cancels the context of a running session, which triggers the
// WebSocket close and ends the K8s exec stream.
func (m *SessionManager) Interrupt(sessionID string) error {
	v, ok := m.sessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found or already closed", sessionID)
	}
	v.(*ActiveSession).cancel()
	return nil
}

// ListActive returns a snapshot of all active session records.
func (m *SessionManager) ListActive() []*commonmodels.TerminalSessionRecord {
	var records []*commonmodels.TerminalSessionRecord
	m.sessions.Range(func(_, v any) bool {
		records = append(records, v.(*ActiveSession).Record)
		return true
	})
	return records
}

// GetRecorder returns the AsciicastRecorder for a running session.
func (m *SessionManager) GetRecorder(sessionID string) (*AsciicastRecorder, bool) {
	v, ok := m.sessions.Load(sessionID)
	if !ok {
		return nil, false
	}
	return v.(*ActiveSession).Recorder, true
}
