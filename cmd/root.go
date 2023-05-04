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
	"fmt"
	"github.com/dhbin/ra/config"
	"github.com/siddontang/go-log/log"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
)

var (
	host     string
	port     int
	username string
	password string

	startBinlogName string
	stopBinlogName  string
	startPosition   uint32
	stopPosition    uint32
	startDatetime   string
	stopDatetime    string

	database string
	tables   []string
	sqlTypes []string
	ddl      bool

	out   string
	local bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "ra",
	Short: "数据库工具",
	Long: `数据库工具
支持binlog数据闪回、binlog转sql等等

支持mysql数据库版本：
5.5.x
5.6.x
5.7.x
8.0.x

binlog转sql例子：
ra tosql --host 127.0.0.1 -u root -p 123456 --start-file mysql-bin.000001

binlog生成恢复sql例子：
ra flashback --host 127.0.0.1 -u root -p 123456 --start-file mysql-bin.000001

解析本地binlog例子：
ra tosql --host 127.0.0.1 -u root -p 123456 --start-file ./mysql-bin.000001 --local

注：解析本地binlog也需要提供数据库信息，用于获取表信息
`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(0)
	}
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.Version = fmt.Sprintf("%s %s %s %s %s", config.Version, runtime.GOOS, runtime.GOARCH, runtime.Version(), config.BuildTime)
}

func parseBinlogCommonFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&host, "host", "127.0.0.1", "数据库host")
	cmd.PersistentFlags().IntVarP(&port, "port", "P", 3306, "数据库端口")
	cmd.PersistentFlags().StringVarP(&username, "username", "u", "", "数据库用户名")
	cmd.PersistentFlags().StringVarP(&password, "password", "p", "", "数据库密码")
	_ = cmd.MarkPersistentFlagRequired("host")
	_ = cmd.MarkPersistentFlagRequired("username")
	_ = cmd.MarkPersistentFlagRequired("password")

	cmd.PersistentFlags().StringVar(&startBinlogName, "start-file", "", "起始解析文件。必须。只需文件名，无需全路径，local模式时，该参数为文件路径")
	cmd.PersistentFlags().StringVar(&stopBinlogName, "stop-file", "", "终止解析文件。可选。默认为start-file同一个文件")
	cmd.PersistentFlags().Uint32Var(&startPosition, "start-position", 4, "起始解析位置。可选。默认为start-file的起始位置")
	cmd.PersistentFlags().Uint32Var(&stopPosition, "stop-position", 0, "终止解析位置。可选。默认为stop-file的最末位置")
	cmd.PersistentFlags().StringVar(&startDatetime, "start-datetime", "", "起始解析时间'。可选。格式'%Y-%m-%d %H:%M:%S。默认不过滤")
	cmd.PersistentFlags().StringVar(&stopDatetime, "stop-datetime", "", "终止解析时间。可选。格式'%Y-%m-%d %H:%M:%S'。默认不过滤")
	_ = cmd.MarkPersistentFlagRequired("start-file")

	cmd.PersistentFlags().StringVarP(&database, "database", "d", "", "只解析目标db的sql，多个库用空格隔开，如-d db1 db2。可选。默认支持所有数据库")
	cmd.PersistentFlags().StringSliceVarP(&tables, "tables", "t", []string{}, "只解析目标table的sql，多张表用空格隔开，如-t tbl1 tbl2。可选。默认支持所有表，当database配置为空时，支持跨库重名的表")
	cmd.PersistentFlags().StringSliceVar(&sqlTypes, "only-type", []string{"insert", "update", "delete"}, "只解析指定类型。支持insert,update,delete。多个类型用逗号隔开，如--sql-type insert,delete。可选。默认为增删改都解析")

	cmd.PersistentFlags().StringVarP(&out, "out", "o", "", "输出sql文件，默认stdout")
	cmd.PersistentFlags().BoolVar(&local, "local", false, "解析本地binlog文件")

}

func buildBinlogConfig() config.BinlogConfig {
	binlogConfig := config.BinlogConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,

		StartBinlogName: startBinlogName,
		StopBinlogName:  stopBinlogName,
		StartPosition:   startPosition,
		StopPosition:    stopPosition,

		Database: database,
		Tables:   tables,
		SqlTypes: sqlTypes,
		DDL:      ddl,

		Out:   out,
		Local: local,
	}

	if binlogConfig.StopBinlogName == "" {
		binlogConfig.StopBinlogName = binlogConfig.StartBinlogName
	}

	if startDatetime != "" {
		startDateTimeTmp, err := time.ParseInLocation("2006-01-02 15:04:05", startDatetime, time.Local)
		if err != nil {
			log.Panic(err)
		}
		binlogConfig.StartDatetime = &startDateTimeTmp
	}

	if stopDatetime != "" {
		stopDatetimeTmp, err := time.ParseInLocation("2006-01-02 15:04:05", stopDatetime, time.Local)
		if err != nil {
			log.Panic(err)
		}
		binlogConfig.StopDatetime = &stopDatetimeTmp
	}

	return binlogConfig
}
