package batchputs_test

import (
	"fmt"

	"time"

	"github.com/theplant/batchputs"
)

/*
With this example, We created 30k records with 3 columns each, and inserts it into database in batch. and then we updates 20k records.

semaphoreci.com runs this example for inserting 30k records and updates 20k records totally less than 2 seconds.

```
=== RUN   ExamplePut_perf
--- PASS: ExamplePut_perf (1.73s)
```

[![Build Status](https://semaphoreci.com/api/v1/theplant/batchputs/branches/master/badge.svg)](https://semaphoreci.com/theplant/batchputs)

*/
func ExamplePut_perf() {
	db := openAndMigrate()
	rows := [][]interface{}{}
	for i := 0; i < 30000; i++ {
		rows = append(rows, []interface{}{
			fmt.Sprintf("CODE_%d", i),
			fmt.Sprintf("short name %d", i),
			i,
		})
	}
	columns := []string{"code", "short_name", "count"}

	start := time.Now()
	err := batchputs.Put(db.DB(), "postgres", "countries", "code", columns, rows)
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
	err = batchputs.Put(db.DB(), "postgres", "countries", "code", columns, rows)
	if err != nil {
		panic(err)
	}
	duration = time.Since(start)
	fmt.Println("Updates 20000 records using less than 3 seconds:", duration.Seconds() < 3)

	//Output:
	// Inserts 30000 records using less than 3 seconds: true
	// Updates 20000 records using less than 3 seconds: true

}
