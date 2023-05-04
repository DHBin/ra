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

package binlog

import (
	"fmt"
	"github.com/dhbin/ra/binlog/event"
	"github.com/dhbin/ra/binlog/parse"
	"github.com/dhbin/ra/config"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strconv"
)

func newCanal(config *config.BinlogConfig) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Host + ":" + strconv.Itoa(config.Port)
	cfg.User = config.Username
	cfg.Password = config.Password
	cfg.Logger = log.NewDefault(&event.DiscardLogHandler{})

	cfg.Dump.ExecutionPath = ""

	// 数据库过滤配置
	if config.Database != "" {
		if len(config.Tables) == 0 {
			cfg.IncludeTableRegex = []string{config.Database + "\\..*"}
		} else {
			includeTableRegex := make([]string, len(config.Tables))
			for i, table := range config.Tables {
				includeTableRegex[i] = config.Database + "\\." + table + "$"
			}
			cfg.IncludeTableRegex = includeTableRegex
		}
	} else {
		if len(config.Tables) != 0 {
			includeTableRegex := make([]string, len(config.Tables))
			for i, table := range config.Tables {
				includeTableRegex[i] = ".*\\." + table + "$"
			}
			cfg.IncludeTableRegex = includeTableRegex
		}
	}

	c, err := canal.NewCanal(cfg)
	return c, err
}

func ToSql(config *config.BinlogConfig) error {
	c, err := newCanal(config)
	if err != nil {
		return err
	}

	done := make(chan interface{})
	handler := event.ToSqlHandler{}
	handler.Config = config
	handler.Done = done
	out, err := config.GetOut()
	if err != nil {
		return err
	}
	handler.Out = out

	go func() {
		if config.Local {
			parser := parse.NewLocalFileParser(config)
			err := parser.Run(&handler)
			done <- err
		} else {
			// Start canal
			c.SetEventHandler(&handler)
			err = c.RunFrom(mysql.Position{Name: config.StartBinlogName, Pos: config.StartPosition})
			if err != nil {
				done <- err
			}
		}
	}()

	switch i := (<-done).(type) {
	case error:
		fmt.Println(i.Error())
	default:
	}
	c.Close()
	return nil
}

func Flashback(config *config.BinlogConfig) error {
	c, err := newCanal(config)
	if err != nil {
		return err
	}

	done := make(chan interface{})
	handler := event.FlashbackHandler{}
	handler.Config = config
	handler.Done = done

	out, err := config.GetOut()
	if err != nil {
		return err
	}
	handler.Out = out

	go func() {

		if config.Local {
			parser := parse.NewLocalFileParser(config)
			err := parser.Run(&handler)
			done <- err
		} else {
			c.SetEventHandler(&handler)
			// Start canal
			err = c.RunFrom(mysql.Position{Name: config.StartBinlogName, Pos: config.StartPosition})
			if err != nil {
				done <- err
			}
		}
	}()

	switch i := (<-done).(type) {
	case error:
		fmt.Println(i.Error())
	default:
	}
	c.Close()
	return nil
}
