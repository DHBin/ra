/*
 * Copyright 2023 The Ra Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package event

import (
	"fmt"
	"github.com/dhbin/ra/binlog/sql"
	"github.com/dhbin/ra/config"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"io"
)

// ToSqlHandler 生成sql
type ToSqlHandler struct {
	BaseHandler
}

// FlashbackHandler 闪回sql
type FlashbackHandler struct {
	BaseHandler
}

type BaseHandler struct {
	canal.DummyEventHandler
	Config *config.BinlogConfig
	Done   chan interface{}
	Out    io.Writer

	isDone         bool
	currentLogName string
}

func (h *BaseHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	h.currentLogName = string(rotateEvent.NextLogName)
	if h.ignore(header) {
		return nil
	}
	return nil
}

func (h *BaseHandler) OnDDL(header *replication.EventHeader, _ mysql.Position, _ *replication.QueryEvent) error {
	if h.ignore(header) {
		return nil
	}
	return nil
}

func (h *BaseHandler) OnXID(header *replication.EventHeader, _ mysql.Position) error {
	if h.ignore(header) {
		return nil
	}
	return nil
}

func (h *BaseHandler) OnGTID(header *replication.EventHeader, _ mysql.GTIDSet) error {
	if h.ignore(header) {
		return nil
	}
	return nil
}

func (h *BaseHandler) OnTableChanged(header *replication.EventHeader, _ string, _ string) error {
	if h.ignore(header) {
		return nil
	}
	return nil
}

func (h *BaseHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, _ bool) error {
	h.currentLogName = pos.Name
	if h.ignore(header) {
		return nil
	}
	return nil
}

func (h *BaseHandler) ignore(header *replication.EventHeader) bool {
	if h.isDone {
		return true
	}
	if h.Config.StopPosition != 0 && header.LogPos >= h.Config.StopPosition {
		h.isDone = true
		h.Done <- ""
	}

	if h.Config.StartDatetime != nil && h.Config.StartDatetime.Unix() > int64(header.Timestamp) {
		return true
	}

	if h.Config.StopBinlogName == h.currentLogName || (h.currentLogName == "") {
		if h.Config.StopDatetime != nil && h.Config.StopDatetime.Unix() <= int64(header.Timestamp) {
			h.isDone = true
			h.Done <- ""
		}
	}

	return false
}

func (h *ToSqlHandler) OnDDL(header *replication.EventHeader, _ mysql.Position, queryEvent *replication.QueryEvent) error {
	if h.ignore(header) {
		return nil
	}
	if h.Config.DDL {
		_, _ = fmt.Fprintln(h.Out, string(queryEvent.Query))
	}
	return nil
}

func (h *ToSqlHandler) OnRow(e *canal.RowsEvent) error {
	if h.ignore(e.Header) {
		return nil
	}
	switch e.Action {
	case canal.InsertAction:
		if !h.Config.SupportSqlType(canal.InsertAction) {
			return nil
		}
		_, _ = fmt.Fprintf(h.Out, "%s # pos %d timestamp %d\n", sql.BuildInsertSql(e.Table, e.Rows[0]), e.Header.LogPos, e.Header.Timestamp)
	case canal.UpdateAction:
		if !h.Config.SupportSqlType(canal.UpdateAction) {
			return nil
		}
		_, _ = fmt.Fprintf(h.Out, "%s # pos %d timestamp %d\n", sql.BuildUpdateSql(e.Table, e.Rows[0], e.Rows[1]), e.Header.LogPos, e.Header.Timestamp)
	case canal.DeleteAction:
		if !h.Config.SupportSqlType(canal.DeleteAction) {
			return nil
		}
		_, _ = fmt.Fprintf(h.Out, "%s # pos %d timestamp %d\n", sql.BuildDeleteSql(e.Table, e.Rows[0]), e.Header.LogPos, e.Header.Timestamp)
	}
	return nil
}

func (h *FlashbackHandler) OnRow(e *canal.RowsEvent) error {
	if h.ignore(e.Header) {
		return nil
	}
	switch e.Action {
	case canal.InsertAction:
		if !h.Config.SupportSqlType(canal.InsertAction) {
			return nil
		}
		_, _ = fmt.Fprintf(h.Out, "%s # pos %d timestamp %d\n", sql.BuildDeleteSql(e.Table, e.Rows[0]), e.Header.LogPos, e.Header.Timestamp)
	case canal.UpdateAction:
		if !h.Config.SupportSqlType(canal.UpdateAction) {
			return nil
		}
		_, _ = fmt.Fprintf(h.Out, "%s # pos %d timestamp %d\n", sql.BuildUpdateSql(e.Table, e.Rows[1], e.Rows[0]), e.Header.LogPos, e.Header.Timestamp)
	case canal.DeleteAction:
		if !h.Config.SupportSqlType(canal.DeleteAction) {
			return nil
		}
		_, _ = fmt.Fprintf(h.Out, "%s # pos %d timestamp %d\n", sql.BuildInsertSql(e.Table, e.Rows[0]), e.Header.LogPos, e.Header.Timestamp)
	}
	return nil
}

type DiscardLogHandler struct {
}

func (h *DiscardLogHandler) Write(_ []byte) (n int, err error) {
	return 0, err
}

func (h *DiscardLogHandler) Close() error {
	return nil
}
