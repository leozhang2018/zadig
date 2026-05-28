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
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"
)

const (
	// maxUncompressedSize is the maximum uncompressed cast data size before truncation.
	maxUncompressedSize = 50 * 1024 * 1024 // 50MB

	// maxSearchContentSize is the maximum plain-text size stored for full-text search.
	maxSearchContentSize = 2 * 1024 * 1024 // 2MB

	// subscriberChanSize is the buffer size for each watcher channel.
	subscriberChanSize = 256
)

// ansiEscapeRe matches ANSI/VT100 control sequences for stripping.
var ansiEscapeRe = regexp.MustCompile(`\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])`)

// asciicastHeader is the first line of an asciicast v2 file.
type asciicastHeader struct {
	Version   int    `json:"version"`
	Width     uint16 `json:"width"`
	Height    uint16 `json:"height"`
	Timestamp int64  `json:"timestamp"`
	Title     string `json:"title,omitempty"`
}

// AsciicastRecorder records a terminal session in asciicast v2 format and
// broadcasts raw stdout bytes to live watchers.
type AsciicastRecorder struct {
	startTime      time.Time
	buf            bytes.Buffer // asciicast v2 event lines
	plainBuf       bytes.Buffer // ANSI-stripped plain text (for SearchContent)
	mu             sync.Mutex
	uncompressedSz int64
	truncated      bool
	subscribers    []chan []byte // live watcher channels
}

// NewAsciicastRecorder creates a recorder and writes the asciicast v2 header.
func NewAsciicastRecorder(width, height uint16, title string) *AsciicastRecorder {
	r := &AsciicastRecorder{
		startTime: time.Now(),
	}
	hdr := asciicastHeader{
		Version:   2,
		Width:     width,
		Height:    height,
		Timestamp: r.startTime.Unix(),
		Title:     title,
	}
	b, _ := json.Marshal(hdr)
	r.buf.Write(b)
	r.buf.WriteByte('\n')
	r.uncompressedSz = int64(r.buf.Len())
	return r
}

// WriteOutput records a stdout event and broadcasts it to all live watchers.
// Once the uncompressed size exceeds maxUncompressedSize the cast recording
// stops (but watchers continue to receive data).
func (r *AsciicastRecorder) WriteOutput(data []byte) {
	if len(data) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	// Build watcher payload (raw bytes copy).
	payload := make([]byte, len(data))
	copy(payload, data)

	// Broadcast to live watchers (non-blocking; drop if watcher is slow).
	for _, ch := range r.subscribers {
		select {
		case ch <- payload:
		default:
		}
	}

	// Accumulate plain text for search index.
	if r.plainBuf.Len() < maxSearchContentSize {
		stripped := ansiEscapeRe.ReplaceAll(data, nil)
		remaining := maxSearchContentSize - r.plainBuf.Len()
		if len(stripped) > remaining {
			stripped = stripped[:remaining]
		}
		r.plainBuf.Write(stripped)
	}

	// Stop recording cast data once truncation threshold is reached.
	if r.truncated {
		return
	}

	ts := time.Since(r.startTime).Seconds()
	line := fmt.Sprintf("[%.6f,\"o\",%s]\n", ts, jsonStringBytes(data))
	if r.uncompressedSz+int64(len(line)) > maxUncompressedSize {
		r.truncated = true
		return
	}
	r.buf.WriteString(line)
	r.uncompressedSz += int64(len(line))
}

// WriteResize records a terminal resize event.
func (r *AsciicastRecorder) WriteResize(cols, rows uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.truncated {
		return
	}
	ts := time.Since(r.startTime).Seconds()
	line := fmt.Sprintf("[%.6f,\"r\",\"%dx%d\"]\n", ts, cols, rows)
	if r.uncompressedSz+int64(len(line)) > maxUncompressedSize {
		r.truncated = true
		return
	}
	r.buf.WriteString(line)
	r.uncompressedSz += int64(len(line))
}

// Subscribe registers a new live watcher and returns a read-only channel that
// delivers raw stdout bytes. The caller must call Unsubscribe when done.
func (r *AsciicastRecorder) Subscribe() <-chan []byte {
	ch := make(chan []byte, subscriberChanSize)
	r.mu.Lock()
	r.subscribers = append(r.subscribers, ch)
	r.mu.Unlock()
	return ch
}

// Unsubscribe removes a previously registered watcher channel.
func (r *AsciicastRecorder) Unsubscribe(ch <-chan []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	newSubs := r.subscribers[:0]
	for _, s := range r.subscribers {
		if s != ch {
			newSubs = append(newSubs, s)
		} else {
			close(s)
		}
	}
	r.subscribers = newSubs
}

// Snapshot returns a copy of the asciicast v2 content recorded so far
// (uncompressed). Used to fast-forward new live watchers to the current state.
func (r *AsciicastRecorder) Snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := make([]byte, r.buf.Len())
	copy(snapshot, r.buf.Bytes())
	return snapshot
}

// Flush gzip-compresses the buffered cast data, extracts the search content,
// and returns both. Called once after the session ends.
func (r *AsciicastRecorder) Flush() (compressed []byte, rawSize int64, searchContent string, truncated bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rawSize = r.uncompressedSz
	truncated = r.truncated
	searchContent = r.plainBuf.String()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(r.buf.Bytes())
	_ = gz.Close()
	compressed = buf.Bytes()
	return
}

// jsonStringBytes encodes p as a JSON string value (including quotes).
func jsonStringBytes(p []byte) []byte {
	b, _ := json.Marshal(string(p))
	return b
}
