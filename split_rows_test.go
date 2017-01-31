package batchputs

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/jinzhu/gorm"

	"os"

	"github.com/theplant/testingutils"
)

var splitCases = []struct {
	maxCell  int
	expected [][][]interface{}
}{
	{
		1,
		[][][]interface{}{
			[][]interface{}{{11, 12, 13}},
			[][]interface{}{{21, 22, 23}},
			[][]interface{}{{31, 32, 33}},
		},
	},
	{
		2,
		[][][]interface{}{
			[][]interface{}{{11, 12, 13}},
			[][]interface{}{{21, 22, 23}},
			[][]interface{}{{31, 32, 33}},
		},
	},
	{
		5,
		[][][]interface{}{
			[][]interface{}{{11, 12, 13}},
			[][]interface{}{{21, 22, 23}},
			[][]interface{}{{31, 32, 33}},
		},
	},
	{
		6,
		[][][]interface{}{
			[][]interface{}{
				{11, 12, 13},
				{21, 22, 23},
			},
			[][]interface{}{
				{31, 32, 33},
			},
		},
	},
	{
		7,
		[][][]interface{}{
			[][]interface{}{
				{11, 12, 13},
				{21, 22, 23},
			},
			[][]interface{}{
				{31, 32, 33},
			},
		},
	},
	{
		9,
		[][][]interface{}{
			[][]interface{}{
				{11, 12, 13},
				{21, 22, 23},
				{31, 32, 33},
			},
		},
	},
	{
		100,
		[][][]interface{}{
			[][]interface{}{
				{11, 12, 13},
				{21, 22, 23},
				{31, 32, 33},
			},
		},
	},
}

type Three struct {
	C1 int
	C2 int
	C3 int
}

func TestSplitRowsForMaxCell(t *testing.T) {

	d, err := gorm.Open(os.Getenv("DB_DIALECT"), os.Getenv("DB_PARAMS"))
	d.DropTable(&Three{})
	d.AutoMigrate(&Three{})
	d.LogMode(true)

	if err != nil {
		panic(err)
	}
	sqb := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	for _, c := range splitCases {
		batchRows, err1 := splitRowsForMaxCell(sqb, d.DB(), "threes", [][]interface{}{
			[]interface{}{11, 12, 13},
			[]interface{}{21, 22, 23},
			[]interface{}{31, 32, 33},
		}, c.maxCell)
		if err1 != nil {
			panic(err1)
		}
		diff := testingutils.PrettyJsonDiff(c.expected, batchRows)
		if len(diff) > 0 {
			t.Error(diff)
		}
	}

}
