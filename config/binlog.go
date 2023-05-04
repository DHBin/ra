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

package config

import (
	"io"
	"os"
	"strings"
	"time"
)

type BinlogConfig struct {
	Host     string
	Port     int
	Username string
	Password string

	StartBinlogName string
	StopBinlogName  string
	StartPosition   uint32
	StopPosition    uint32
	StartDatetime   *time.Time
	StopDatetime    *time.Time

	Database string
	Tables   []string
	SqlTypes []string
	DDL      bool

	Out   string
	Local bool

	supportSqlTypeMap map[string]bool
}

func (h *BinlogConfig) SupportSqlType(sqlType string) bool {

	if h.supportSqlTypeMap == nil {
		h.supportSqlTypeMap = make(map[string]bool)
		for _, sqlType := range h.SqlTypes {
			sqlTypeLower := strings.ToLower(sqlType)
			h.supportSqlTypeMap[sqlTypeLower] = true
		}
	}
	return h.supportSqlTypeMap[strings.ToLower(sqlType)]
}

func (h *BinlogConfig) GetOut() (io.Writer, error) {
	if h.Out == "" {
		return os.Stdout, nil
	} else {
		file, err := os.Create(h.Out)
		return file, err
	}
}
