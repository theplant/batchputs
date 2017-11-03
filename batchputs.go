/*

`batchputs.Put` utilize delete and insert sql with multiple values to do updates to database:

```
DELETE FROM tab WHERE c1 IN ("11", "21", "31"...)
INSERT INTO tab (c1, c2) VALUES ("11", "12"),("21", "22"),("31", "32"),(...)
```

With the minimum numbers of sqls (but very large body) sent to database for inserts/deletes, It can achieve great performance.

*/
package batchputs

import (
	"database/sql"
	"fmt"
	"log"

	sq "github.com/Masterminds/squirrel"
)

var Verbose bool

type RowWillChange func(row []interface{}, columns []string)

func Put(
	db sq.BaseRunner,
	driverName string,
	tableName string,
	primaryKeyColumn string,
	columns []string,
	rows [][]interface{}) (err error) {
	err = CollectChangePut(
		db,
		driverName,
		tableName,
		primaryKeyColumn,
		columns,
		rows,
		nil,
	)
	return
}

func CollectChangePut(
	db sq.BaseRunner,
	driverName string,
	tableName string,
	primaryKeyColumn string,
	columns []string,
	rows [][]interface{},
	rowWillChange RowWillChange,
) (err error) {
	return CollectChangePutWithMaxSQLParamsCount(
		db,
		driverName,
		tableName,
		primaryKeyColumn,
		columns,
		rows,
		rowWillChange,
		65535,
	)
}

func CollectChangePutWithMaxSQLParamsCount(
	db sq.BaseRunner,
	driverName string,
	tableName string,
	primaryKeyColumn string,
	columns []string,
	rows [][]interface{},
	rowWillChange RowWillChange,
	maxSQLParamsCount int,
) (err error) {

	if len(rows) == 0 {
		return
	}

	pkIndex := primaryKeyIndex(columns, primaryKeyColumn)
	if pkIndex < 0 {
		err = fmt.Errorf("primary key column %+v must exists in columns %+v", primaryKeyColumn, columns)
		return
	}

	if len(columns) != len(rows[0]) {
		err = fmt.Errorf("columns %+v count not match for rows count %+v", columns, rows[0])
		return
	}

	sqb := sq.StatementBuilder

	if driverName == "postgres" {
		sqb = sqb.PlaceholderFormat(sq.Dollar)
	}
	max := maxSQLParamsCount
	if max == 0 {
		max = 65536
	}
	// panic: pq: got 210000 parameters but PostgreSQL only supports 65535 parameters
	batchedRows, err1 := splitRowsForMaxCell(sqb, db, tableName, rows, max)
	if err1 != nil {
		err = err1
		return
	}

	for _, bRows := range batchedRows {
		if Verbose {
			log.Printf("batchputs: in batch size: %#+v, max_sql_params_count: %d\n", len(bRows), max)
		}
		var priVals = primaryValues(pkIndex, bRows)
		allColumns, allColumnRows, changedPriVals, err1 := changedRows(sqb, db, tableName, columns, bRows, primaryKeyColumn, priVals)
		if err1 != nil {
			err = err1
			return
		}

		if len(allColumnRows) == 0 {
			continue
		}

		if len(changedPriVals) > 0 {
			deletes := sqb.Delete(tableName).Where(sq.Eq{primaryKeyColumn: changedPriVals})
			if Verbose {
				deletesSQL, deletesArgs, _ := deletes.ToSql()
				log.Println(deletesSQL, deletesArgs)
			}

			_, err = deletes.RunWith(db).Exec()
			if err != nil {
				return
			}
		}
		inserts := sqb.Insert(tableName).Columns(allColumns...)
		for _, row := range allColumnRows {
			if rowWillChange != nil {
				rowWillChange(row, allColumns)
			}
			inserts = inserts.Values(row...)
		}

		if Verbose {
			insertsSQL, insertArgs, _ := inserts.ToSql()
			log.Println(insertsSQL, insertArgs)
		}

		_, err = inserts.RunWith(db).Exec()
		if err != nil {
			return
		}
	}

	return
}

func changedRows(
	sqb sq.StatementBuilderType,
	db sq.BaseRunner,
	tableName string,
	columns []string,
	rows [][]interface{},
	primaryKeyColumn string,
	priVals []interface{}) (allColumns []string,
	allColumnsRows [][]interface{},
	deletePriVals []interface{},
	err error) {

	caches := map[interface{}][]interface{}{}
	var sRows *sql.Rows

	selects := sqb.Select("*").From(tableName).Where(sq.Eq{primaryKeyColumn: priVals})
	if Verbose {
		selectsSQL, selectsArgs, _ := selects.ToSql()
		log.Println(selectsSQL, selectsArgs)
	}

	sRows, err = selects.RunWith(db).Query()
	if err != nil {
		return
	}
	defer sRows.Close()

	var qColumns []string
	qColumns, err = sRows.Columns()
	if err != nil {
		return
	}
	err = checkColumns(tableName, columns, qColumns)
	if err != nil {
		return
	}

	othercs := otherColumns(columns, qColumns)
	allColumns = append(columns, othercs...)
	for sRows.Next() {
		var qrow = make([]interface{}, len(qColumns))

		for i, _ := range qrow {
			var s = sql.NullString{}
			qrow[i] = &s
		}

		err = sRows.Scan(qrow...)

		if err != nil {
			return
		}

		qPriIndex := primaryKeyIndex(qColumns, primaryKeyColumn)
		caches[qrow[qPriIndex].(*sql.NullString).String] = qrow
	}

	// log.Printf("caches: %#+v\n", caches)
	for _, row := range rows {
		priIndex := primaryKeyIndex(columns, primaryKeyColumn)
		qrow := caches[fmt.Sprintf("%+v", row[priIndex])]
		if len(qrow) > 0 {
			qvals := columnsValues(columns, qColumns, qrow)
			for i, qr := range qvals {
				qvals[i] = qr.(*sql.NullString).String
			}
			qvalsStr := fmt.Sprintf("%+v", qvals)
			updateValsStr := fmt.Sprintf("%+v", row)
			// log.Printf("qvalsStr: %#+v, %#+v, %#+v\n", qvalsStr, updateValsStr, qvalsStr == updateValsStr)
			if qvalsStr == updateValsStr {
				continue
			}
			deletePriVals = append(deletePriVals, row[priIndex])
		}

		var allRow []interface{}
		allRow = append(allRow, row...)
		if len(qrow) > 0 {
			allRow = append(allRow, columnsValues(othercs, qColumns, qrow)...)
		} else {
			for _, _ = range othercs {
				allRow = append(allRow, nil)
			}
		}
		allColumnsRows = append(allColumnsRows, allRow)
	}

	return
}

func otherColumns(columns []string, allColumns []string) (others []string) {
	for _, ac := range allColumns {
		found := false
		for _, c := range columns {
			if ac == c {
				found = true
				break
			}
		}
		if !found {
			others = append(others, ac)
		}
	}
	return
}

func checkColumns(tableName string, columns []string, allColumns []string) (err error) {
	for _, c := range columns {
		found := false
		for _, dc := range allColumns {
			if c == dc {
				found = true
				break
			}
		}
		if !found {
			err = fmt.Errorf("some columns %+v not in table %+v columns %+v", columns, tableName, allColumns)
			return
		}
	}
	return
}

func columnsValues(selectedColumns []string, allColumns []string, row []interface{}) (vals []interface{}) {
	for _, sc := range selectedColumns {
		for i, ac := range allColumns {
			if sc == ac {
				vals = append(vals, row[i])
			}
		}
	}
	return
}

func primaryKeyIndex(columns []string, primaryKeyColumn string) (index int) {
	for i, c := range columns {
		if c == primaryKeyColumn {
			return i
		}
	}
	return -1
}

func primaryValues(pkIndex int, rows [][]interface{}) (values []interface{}) {
	for _, row := range rows {
		values = append(values, row[pkIndex])
	}
	return
}

func splitRowsForMaxCell(sqb sq.StatementBuilderType, db sq.BaseRunner, tableName string, rows [][]interface{}, maxCellCount int) (batchedRows [][][]interface{}, err error) {
	var emptyRows *sql.Rows
	emptyRows, err = sqb.Select("*").From(tableName).Where(sq.NotEq{"1": "1"}).RunWith(db).Query()
	if err != nil {
		return
	}
	defer emptyRows.Close()
	var fullColumns []string
	fullColumns, err = emptyRows.Columns()
	if err != nil {
		return
	}
	columnsCount := len(fullColumns)

	// postgresql max sql parameters count is 65535
	maxRowCount := maxCellCount / columnsCount
	if maxRowCount == 0 {
		maxRowCount = 1
	}

	lastBatch := [][]interface{}{}
	for i, row := range rows {
		lastBatch = append(lastBatch, row)
		if (i+1)%maxRowCount == 0 {
			batchedRows = append(batchedRows, lastBatch)
			lastBatch = [][]interface{}{}
		}
	}

	if len(lastBatch) > 0 {
		batchedRows = append(batchedRows, lastBatch)
	}

	return
}
