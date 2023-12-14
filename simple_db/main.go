package main

import (
	bmg "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	metadata_manager "metadata_management"
	"parser"
	"planner"
	"query"
	"record_manager"
	"tx"
)

func printStats(n int, p planner.Plan) {
	fmt.Printf("Here are the stats for plan p %d\n", n)
	fmt.Printf("\tR(p%d):%d\n", n, p.RecordsOutput())
	fmt.Printf("\tB(p%d):%d\n", n, p.BlocksAccessed())
}

func createStudentTable() (*tx.Transation, *metadata_manager.MetaDataManager) {
	file_manager, _ := fm.NewFileManager("student", 2048)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)
	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := record_manager.NewSchema()
	mdm := metadata_manager.NewMetaDataManager(false, tx)

	sch.AddStringField("name", 16)
	sch.AddIntField("id")
	layout := record_manager.NewLayoutWithSchema(sch)

	ts := query.NewTableScan(tx, "student", layout)
	ts.BeforeFirst()
	for i := 1; i <= 3; i++ {
		ts.Insert() //指向一个可用插槽
		ts.SetInt("id", i)
		if i == 1 {
			ts.SetString("name", "Tom")
		}
		if i == 2 {
			ts.SetString("name", "Jim")
		}
		if i == 3 {
			ts.SetString("name", "John")
		}
	}

	mdm.CreateTable("student", sch, tx)

	exam_sch := record_manager.NewSchema()

	exam_sch.AddIntField("stuid")
	exam_sch.AddStringField("exam", 16)
	exam_sch.AddStringField("grad", 16)
	exam_layout := record_manager.NewLayoutWithSchema(exam_sch)

	ts = query.NewTableScan(tx, "exam", exam_layout)
	ts.BeforeFirst()

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 1)
	ts.SetString("exam", "math")
	ts.SetString("grad", "A")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 1)
	ts.SetString("exam", "algorithm")
	ts.SetString("grad", "B")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 2)
	ts.SetString("exam", "writing")
	ts.SetString("grad", "C")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 2)
	ts.SetString("exam", "physics")
	ts.SetString("grad", "C")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 3)
	ts.SetString("exam", "chemical")
	ts.SetString("grad", "B")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 3)
	ts.SetString("exam", "english")
	ts.SetString("grad", "C")

	mdm.CreateTable("exam", exam_sch, tx)

	return tx, mdm
}

func main() {
	//构造 student 表
	tx, mdm := createStudentTable()
	queryStr := "select name from student, exam where id = stuid and grad=\"A\""
	p := parser.NewSQLParser(queryStr)
	queryData := p.Query()
	test_planner := planner.CreateBasicQueryPlanner(mdm)
	test_plan := test_planner.CreatePlan(queryData, tx)
	test_interface := (test_plan.Open())
	test_scan, _ := test_interface.(query.Scan)
	for test_scan.Next() {
		fmt.Printf("name: %s\n", test_scan.GetString("name"))
	}

}
