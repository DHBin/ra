# ra

## 使用方法

```
数据库工具
支持binlog数据闪回、binlog转sql等等

支持mysql数据库版本：
5.5.x
5.6.x
5.7.x
8.0.x

Usage:
  ra [command]

Available Commands:
  flashback   数据闪回
  help        Help about any command
  tosql       通过binlog日志生成sql

Flags:
  -h, --help      help for ra
  -v, --version   version for ra

Use "ra [command] --help" for more information about a command.
```

### 数据闪回

```text
Usage:
  ra flashback [flags]

Flags:
  -d, --database string         只解析目标db的sql，多个库用空格隔开，如-d db1 db2。可选。默认支持所有数据库
  -h, --help                    help for flashback
      --host string             数据库host (default "127.0.0.1")
      --only-type strings       只解析指定类型。支持insert,update,delete。多个类型用逗号隔开，如--sql-type insert,delete。可选。默认为增删改都解析 (default [insert,update,delete])
  -p, --password string         数据库密码
  -P, --port int                数据库端口 (default 3306)
      --start-datetime string   起始解析时间'。可选。格式'%Y-%m-%d %H:%M:%S。默认不过滤
      --start-file string       起始解析文件。必须。只需文件名，无需全路径
      --start-position uint32   起始解析位置。可选。默认为start-file的起始位置 (default 4)
      --stop-datetime string    终止解析时间。可选。格式'%Y-%m-%d %H:%M:%S'。默认不过滤
      --stop-file string        终止解析文件。可选。默认为start-file同一个文件
      --stop-position uint32    终止解析位置。可选。默认为stop-file的最末位置
  -t, --tables strings          只解析目标table的sql，多张表用空格隔开，如-t tbl1 tbl2。可选。默认支持所有表，当database配置为空时，支持跨库重名的表
  -u, --username string         数据库用户名
```

### binlog转sql

```text
Usage:
  ra tosql [flags]

Flags:
  -d, --database string         只解析目标db的sql，多个库用空格隔开，如-d db1 db2。可选。默认支持所有数据库
      --ddl                     是否解析ddl语句
  -h, --help                    help for tosql
      --host string             数据库host (default "127.0.0.1")
      --only-type strings       只解析指定类型。支持insert,update,delete。多个类型用逗号隔开，如--sql-type insert,delete。可选。默认为增删改都解析 (default [insert,update,delete])
  -p, --password string         数据库密码
  -P, --port int                数据库端口 (default 3306)
      --start-datetime string   起始解析时间'。可选。格式'%Y-%m-%d %H:%M:%S。默认不过滤
      --start-file string       起始解析文件。必须。只需文件名，无需全路径
      --start-position uint32   起始解析位置。可选。默认为start-file的起始位置 (default 4)
      --stop-datetime string    终止解析时间。可选。格式'%Y-%m-%d %H:%M:%S'。默认不过滤
      --stop-file string        终止解析文件。可选。默认为start-file同一个文件
      --stop-position uint32    终止解析位置。可选。默认为stop-file的最末位置
  -t, --tables strings          只解析目标table的sql，多张表用空格隔开，如-t tbl1 tbl2。可选。默认支持所有表，当database配置为空时，支持跨库重名的表
  -u, --username string         数据库用户名
```

# 感谢

- 本项目参照了 [danfengcao/binlog2sql](https://github.com/danfengcao/binlog2sql) python版本


# LICENSE

```text
Copyright 2023 The Ra Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
    
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```