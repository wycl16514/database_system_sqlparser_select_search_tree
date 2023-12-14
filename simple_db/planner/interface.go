package planner

import (
	"parser"
	"record_manager"
	"tx"
)

type Plan interface {
	Open() interface{}
	BlocksAccessed() int               //对应 B(s)
	RecordsOutput() int                //对应 R(s)
	DistinctValues(fldName string) int //对应 V(s,F)
	Schema() record_manager.SchemaInterface
}

type QueryPlanner interface {
	CreatePlan(data *parser.QueryData, tx *tx.Transation) Plan
}
