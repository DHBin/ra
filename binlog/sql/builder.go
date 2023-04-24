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

package sql

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	"strings"
)

// BuildInsertSql 构建插入sql
func BuildInsertSql(table *schema.Table, rows []interface{}) string {
	err := check(table, rows, "insert")
	if err != nil {
		return err.Error()
	}
	colLength := len(table.Columns)
	colsName := make([]string, colLength)
	colsVal := make([]string, colLength)
	for i := range table.Columns {
		colsName[i] = table.Columns[i].Name
		colsVal[i] = typeConvertString(&table.Columns[i], rows[i])
	}
	cols := strings.Join(colsName, ", ")
	values := strings.Join(colsVal, ", ")
	sqlTemplate := "insert into `%v`.`%v` (%v) values(%v);"
	return fmt.Sprintf(sqlTemplate, table.Schema, table.Name, cols, values)
}

// BuildDeleteSql 构建删除sql
func BuildDeleteSql(table *schema.Table, rows []interface{}) string {
	err := check(table, rows, "delete")
	if err != nil {
		return err.Error()
	}
	conditions := genAssignment(table, rows)
	sqlTemplate := "delete from `%v`.`%v` where %s limit 1;"
	return fmt.Sprintf(sqlTemplate, table.Schema, table.Name, strings.Join(conditions, " and "))
}

// BuildUpdateSql 构建更新sql
func BuildUpdateSql(table *schema.Table, conditionRow []interface{}, row []interface{}) string {
	err := check(table, row, "update")
	if err != nil {
		return err.Error()
	}
	err = check(table, conditionRow, "update")
	if err != nil {
		return err.Error()
	}
	sqlTemplate := "update `%v`.`%v` set %s where %s limit 1;"
	setValues := strings.Join(genAssignment(table, row), ", ")
	conditions := strings.Join(genAssignment(table, conditionRow), " and ")
	return fmt.Sprintf(sqlTemplate, table.Schema, table.Name, setValues, conditions)
}

func genAssignment(table *schema.Table, rows []interface{}) []string {
	colLength := len(table.Columns)
	conditions := make([]string, colLength)
	for i := range table.Columns {
		conditions[i] = fmt.Sprintf("%s = %s", table.Columns[i].Name, typeConvertString(&table.Columns[i], rows[i]))
	}
	return conditions
}

func check(table *schema.Table, rows []interface{}, action string) error {
	colLength := len(table.Columns)
	rowLength := len(rows)
	if colLength != rowLength {
		return fmt.Errorf("字段不一致，跳过生成%s sql cols: %v values: %v", action, table.Columns, rows)
	}
	return nil
}

func typeConvertString(column *schema.TableColumn, val interface{}) string {
	switch column.Type {
	case schema.TYPE_BIT, schema.TYPE_MEDIUM_INT, schema.TYPE_FLOAT, schema.TYPE_DECIMAL, schema.TYPE_NUMBER:
		return fmt.Sprintf("%v", val)
	case schema.TYPE_JSON:
		return fmt.Sprintf("cast('%s' as json)", val)
	default:
		return fmt.Sprintf("'%v'", val)
	}
}
