package batchputs

import (
	"testing"

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

func TestSplitRowsForMaxCell(t *testing.T) {

	for _, c := range splitCases {
		batchRows := splitRowsForMaxCell([][]interface{}{
			[]interface{}{11, 12, 13},
			[]interface{}{21, 22, 23},
			[]interface{}{31, 32, 33},
		}, c.maxCell)

		diff := testingutils.PrettyJsonDiff(c.expected, batchRows)
		if len(diff) > 0 {
			t.Error(diff)
		}
	}

}
