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

package cmd

import (
	"github.com/dhbin/ra/binlog"
	"github.com/siddontang/go-log/log"
	"github.com/spf13/cobra"
)

// toSqlCmd represents the toSql command
var toSqlCmd = &cobra.Command{
	Use:   "tosql",
	Short: "通过binlog日志生成sql",
	Run: func(cmd *cobra.Command, args []string) {
		binlogConfig := buildBinlogConfig()
		err := binlog.ToSql(&binlogConfig)
		if err != nil {
			log.Panic(err)
		}
	},
}

func init() {
	parseBinlogCommonFlags(toSqlCmd)
	toSqlCmd.PersistentFlags().BoolVar(&ddl, "ddl", false, "是否解析ddl语句")

	rootCmd.AddCommand(toSqlCmd)
}
