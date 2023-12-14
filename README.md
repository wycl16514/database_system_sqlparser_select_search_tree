首先我们需要给原来代码打个补丁，在SelectScan 结构体初始化时需要传入 UpdateScan 接口对象，但很多时候我们需要传入的是 Scan 对象，因此我们需要做一个转换，也就是当初始化 SelectScan 时，如果传入的是 Scan 对象，那么我们就将其封装成 UpdateScan 接口对象，因此在 query 目录下增加一个名为 updatescan_wrapper.go 的文件，在其中输入内容如下:
```go
package query

import (
	"record_manager"
)

type UpdateScanWrapper struct {
	scan Scan
}

func NewUpdateScanWrapper(s Scan) *UpdateScanWrapper {
	return &UpdateScanWrapper{
		scan: s,
	}
}

func (u *UpdateScanWrapper) GetScan() Scan {
	return u.scan
}

func (u *UpdateScanWrapper) SetInt(fldName string, val int) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) SetString(fldName string, val string) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) SetVal(fldName string, val *Constant) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) Insert() {
	//DO NOTHING
}

func (u *UpdateScanWrapper) Delete() {
	//DO NOTHING
}

func (u *UpdateScanWrapper) GetRid() *record_manager.RID {
	return nil
}

func (u *UpdateScanWrapper) MoveToRid(rid *record_manager.RID) {
	// DO NOTHING
}

```
上面代码逻辑简单，如果调用 Scan 对象接口时，他直接调用其 Scan 内部对象的接口，如果调用到 UpdateScan 的接口，那么它什么都不做。完成上面代码后，我们在select_plan.go 中进行一些修改：
```go
func (s *SelectPlan) Open() interface{} {
	scan := s.p.Open()
	updateScan, ok := scan.(query.UpdateScan)
	if !ok {
		updateScanWrapper := query.NewUpdateScanWrapper(scan.(query.Scan))
		return query.NewSelectionScan(updateScanWrapper, s.pred)
	}
	return query.NewSelectionScan(updateScan, s.pred)
}
```
上面代码在创建 SelectScan 对象时，先判断传进来的对象是否能类型转换为 UpdateScan，如果不能，那意味着s.p.Open 获取的是 Scan 对象，因此我们使用前面的代码封装一下再用来创建 SelectScan 对象。完成这里的修改后，我们进入正题。

前面我们在实现 sql 解析器后，在解析完一条查询语句后会创建一个 QueryData 对象，本节我们看看如何根据这个对象构建出合适的查询规划器(Plan)。我们将采取由简单到负责的原则，首先我们直接构建 QueryData 的信息去构建查询规划对象，此时我们不考虑它所构造的查询树是否足够优化，后面我们再慢慢改进构造算法，直到算法能构建出足够优化的查询树。

我们先看一个具体例子，假设我们现在有两个表 STUDENT, EXAM，第一个表包含两个字段分别是学生 id 和姓名：
id     | name
-------- | -----
1  | Tom
2  | Jim
3  | John

第二个表包含的是学生 id,科目名称，考试乘机：
stuid     | exam|grad
-------- | -----|-----|
1  | math|  A|
1  | algorithm| B
2  | writing| C |
2| physics|  C|
3|chemical|B|
3|english| C|

现在我们使用 sql 语句查询所有考试成绩得过 A 的学生：
```sql
select name from STUDENT, EXAM where id = student_id and grad='A'
```
当 sql 解释器读取上面语句后，他就会创建一个 QueryData 结构，里面 Tables 对了就包含两个表的名字，也就是 STUDENT, EXAM。由于这两个表不是视图，因此上面代码中判断 if viewDef != nil 不成立，于是进入 else 部分，也就是代码会为这两个表创建对应的 TablePlan 对象，接下来直接对这两个表执行 Product 操作，也就是将左边表的一行跟右边表的每一行合起来形成新表的一行，Product 操作在 STUDENT 和 EXAM 表后所得结果如下：
id|name|student_id     | exam|grad
--------|-----|-------- | -----|-----|
1|Tom|1|math|A|
1|Tom|1|algorithm|B|
1|Tom|2|writing|A|
1|Tom|2|physics|C|
1|Tom|3|chemical|B|
1|Tom|3|english|A|
.....|....|.....|...|...|

接下来代码创建 ScanSelect 对象在上面的表上，接着获取该表的每一行，然后检测该行的 id 字段是否跟 student_id 字段一样，如果相同，那么查看其 grad 字段，如果该字段是'A'，就将该行的 name 字段显示出来。

下面我们看看如何使用代码把上面描述的流程实现出来。首先我们先对接口进行定义，在 Planner 目录下的 interface.go 文件中增加如下内容：
```go
type QueryPlanner interface {
	CreatePlan(data *query.QueryData, tx tx.Transaction) Plan
}
```
接着在 Planner 目录下创建文件 query_planner.go，同时输入以下代码，代码的实现逻辑将接下来的文章中进行说明：
```go
package planner

import (
	"metadata_management"
	"parser"
	"tx"
)

type BasicQueryPlanner struct {
	mdm *metadata_management.MetaDataManager
}

func CreateBasicQueryPlanner(mdm *metadata_management.MetaDataManager) QueryPlanner {
	return &BasicQueryPlanner{
		mdm: mdm,
	}
}

func (b *BasicQueryPlanner) CreatePlan(data *parser.QueryData, tx *tx.Transaction) Plan {
	//1,直接创建 QueryData 对象中的表
	plans := make([]Plan, 0)
	tables := data.Tables()
	for _, tblname := range tables {
		//获取该表对应视图的 sql 代码
		viewDef := b.mdm.GetViewDef(tblname, tx)
		if viewDef != nil {
			//直接创建表对应的视图
			parser := parser.NewSQLParser(viewDef)
			viewData := parser.Query()
			//递归的创建对应表的规划器
			plans = append(plans, b.CreatePlan(viewData, tx))
		} else {
			plans = append(plans, NewTablePlan(tx, tblname, b.mdm))
		}
	}

	//将所有表执行 Product 操作，注意表的次序会对后续查询效率有重大影响，但这里我们不考虑表的次序，只是按照
	//给定表依次执行 Product 操作，后续我们会在这里进行优化
	p := plans[0]
	plans = plans[1:]

	for _, nextPlan := range plans {
		p = NewProductPlan(p, nextPlan)
	}

	p = NewSelectPlan(p, data.Pred())

	return NewProjectPlan(p, data.Fields())
}

```
上面代码中 QueryData就是解析器在解析 select 语句后生成的对象，它的 Tables 数组包含了 select 语句要查询的表，所以上面代码的 CreatePlan 函数先从 QueryData 对象获得 select 语句要查询的表，然后使用遍历这些表，使用 NewProductPlan 创建这些表对应的 Product 操作，最后在 Product 的基础上我们再创建 SelectPlan，这里我们就相当于使用 where 语句中的条件，在 Product 操作基础上将满足条件的行选出来，最后再创建 ProjectPlan，将在选出的行基础上，将需要的字段选择出来。

下面我们测试一下上面代码的效果，首先在 main.go 中，我们先把 student, exam 两个表构造出来，代码如下：
```go
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
```
然后我们用解析器解析select查询语句生成 QueryData 对象，最后使用BasicQueryPlanner创建好执行树和对应的 Scan 接口对象，最后我们调用 Scan 对象的 Next 接口来获取给定字段，代码如下：
```go
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

```
上面代码运行后所得结果如下：
![请添加图片描述](https://img-blog.csdnimg.cn/direct/4a41ac35844e418e9166d7615c6c0967.png)
从运行结果看到，代码成功执行了 sql 语句并返回了所需要的字段。请感兴趣的同学在 B 站搜索 coding 迪斯尼，通过视频的方式查看我的调试演示过程，这样才能对代码的设计有更好的理解，代码下载：
链接: https://pan.baidu.com/s/16ftSp46cU5NLisScq-ftZg 提取码: js99 


