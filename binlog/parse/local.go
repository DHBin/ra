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

package parse

import (
	"fmt"
	"github.com/dhbin/ra/binlog/event"
	"github.com/dhbin/ra/config"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"strconv"
)

type LocalFileParser struct {
	canal      *canal.Canal
	binlogFile string
}

func (h *LocalFileParser) Run(eventHandler canal.EventHandler) error {
	parser := replication.NewBinlogParser()

	pos := mysql.Position{}
	err := parser.ParseFile(h.binlogFile, 0, func(ev *replication.BinlogEvent) error {
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			err := eventHandler.OnRotate(ev.Header, e)
			if err != nil {
				return err
			}
		case *replication.RowsEvent:
			err := h.handleRowsEvent(ev, eventHandler)
			if err != nil {
				return err
			}
		case *replication.XIDEvent:
			err := eventHandler.OnXID(ev.Header, pos)
			if err != nil {
				return err
			}
		case *replication.MariadbGTIDEvent:
			gtid, err := mysql.ParseMariadbGTIDSet(e.GTID.String())
			if err != nil {
				return err
			}
			err = eventHandler.OnGTID(ev.Header, gtid)
			if err != nil {
				return err
			}
		case *replication.GTIDEvent:
			u, _ := uuid.FromBytes(e.SID)
			gtid, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
			if err != nil {
				return err
			}
			err = eventHandler.OnGTID(ev.Header, gtid)
			if err != nil {
				return err
			}
		case *replication.QueryEvent:
			err := eventHandler.OnDDL(ev.Header, pos, e)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (h *LocalFileParser) handleRowsEvent(e *replication.BinlogEvent, handler canal.EventHandler) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	s := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := h.canal.GetTable(s, table)
	if err != nil {
		return err
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = canal.InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = canal.DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = canal.UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := newRowsEvent(t, action, ev.Rows, e.Header)
	return handler.OnRow(events)
}

func newRowsEvent(table *schema.Table, action string, rows [][]interface{}, header *replication.EventHeader) *canal.RowsEvent {
	e := new(canal.RowsEvent)

	e.Table = table
	e.Action = action
	e.Rows = rows
	e.Header = header

	handleUnsigned(e)

	return e
}

const maxMediumintUnsigned int32 = 16777215

func handleUnsigned(r *canal.RowsEvent) {
	// Handle Unsigned Columns here, for binlog replication, we can't know the integer is unsigned or not,
	// so we use int type but this may cause overflow outside sometimes, so we must convert to the really .
	// unsigned type
	if len(r.Table.UnsignedColumns) == 0 {
		return
	}

	for i := 0; i < len(r.Rows); i++ {
		for _, columnIdx := range r.Table.UnsignedColumns {
			switch value := r.Rows[i][columnIdx].(type) {
			case int8:
				r.Rows[i][columnIdx] = uint8(value)
			case int16:
				r.Rows[i][columnIdx] = uint16(value)
			case int32:
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if value < 0 && r.Table.Columns[columnIdx].Type == schema.TYPE_MEDIUM_INT {
					r.Rows[i][columnIdx] = uint32(maxMediumintUnsigned + value + 1)
				} else {
					r.Rows[i][columnIdx] = uint32(value)
				}
			case int64:
				r.Rows[i][columnIdx] = uint64(value)
			case int:
				r.Rows[i][columnIdx] = uint(value)
			default:
				// nothing to do
			}
		}
	}
}

func NewLocalFileParser(config *config.BinlogConfig) *LocalFileParser {
	p := new(LocalFileParser)
	p.binlogFile = config.StartBinlogName

	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Host + ":" + strconv.Itoa(config.Port)
	cfg.User = config.Username
	cfg.Password = config.Password
	cfg.Logger = log.NewDefault(&event.DiscardLogHandler{})
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil
	}
	p.canal = c
	return p
}
