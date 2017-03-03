

`batchputs.Put` utilize delete and insert sql with multiple values to do updates to database:

```
DELETE FROM tab WHERE c1 IN ("11", "21", "31"...)
INSERT INTO tab (c1, c2) VALUES ("11", "12"),("21", "22"),("31", "32"),(...)
```

With the minimum numbers of sqls (but very large body) sent to database for inserts/deletes, It can achieve great performance.




* [Variables](#variables)
* [Collect Change Put](#collect-change-put)
* [Put](#put)
* [Type Row Will Change](#type-row-will-change)


## Variables
``` go
var Verbose bool
```


## Collect Change Put
``` go
func CollectChangePut(
    db sq.BaseRunner,
    driverName string,
    tableName string,
    primaryKeyColumn string,
    columns []string,
    rows [][]interface{},
    rowWillChange RowWillChange,
) (err error)
```


## Put
``` go
func Put(
    db sq.BaseRunner,
    driverName string,
    tableName string,
    primaryKeyColumn string,
    columns []string,
    rows [][]interface{}) (err error)
```

With this example, We created 30k records with 3 columns each, and inserts it into database in batch. and then we updates 20k records.

semaphoreci.com runs this example for inserting 30k records and updates 20k records totally less than 2 seconds.

```
=== RUN   ExamplePut_perf
--- PASS: ExamplePut_perf (1.73s)
```

[![Build Status](https://semaphoreci.com/api/v1/theplant/batchputs/branches/master/badge.svg)](https://semaphoreci.com/theplant/batchputs)
```go
	db := openAndMigrate()
	// with table
	// CREATE TABLE countries
	// (
	//     code VARCHAR(50) PRIMARY KEY NOT NULL,
	//     short_name TEXT,
	//     special_notes TEXT,
	//     region TEXT,
	//     income_group TEXT,
	//     count INTEGER,
	//     avg_age NUMERIC
	// );
	
	rows := [][]interface{}{}
	for i := 0; i < 30000; i++ {
	    rows = append(rows, []interface{}{
	        fmt.Sprintf("CODE_%d", i),
	        fmt.Sprintf("short name %d", i),
	        i,
	    })
	}
	columns := []string{"code", "short_name", "count"}
	dialect := os.Getenv("DB_DIALECT")
	if len(dialect) == 0 {
	    dialect = "postgres"
	}
	
	start := time.Now()
	err := batchputs.Put(db.DB(), dialect, "countries", "code", columns, rows)
	if err != nil {
	    panic(err)
	}
	duration := time.Since(start)
	fmt.Println("Inserts 30000 records using less than 3 seconds:", duration.Seconds() < 3)
	
	rows = [][]interface{}{}
	for i := 0; i < 20000; i++ {
	    rows = append(rows, []interface{}{
	        fmt.Sprintf("CODE_%d", i),
	        fmt.Sprintf("short name %d", i),
	        i + 1,
	    })
	}
	start = time.Now()
	err = batchputs.Put(db.DB(), dialect, "countries", "code", columns, rows)
	if err != nil {
	    panic(err)
	}
	duration = time.Since(start)
	fmt.Println("Updates 20000 records using less than 3 seconds:", duration.Seconds() < 3)
	
	//Output:
	// Inserts 30000 records using less than 3 seconds: true
	// Updates 20000 records using less than 3 seconds: true
```



## Type: Row Will Change
``` go
type RowWillChange func(row []interface{}, columns []string)
```










