package test

type SimpleTestRecord struct {
	Name string `gorm:"column:Name"`
}

type ComplexRecord struct {
	Name   string           `gorm:"column:Name"`
	Record ComplexSubRecord `gorm:"column:Record;type:RECORD"`
}

type ComplexSubRecord struct {
	Name string `gorm:"column:Name"`
	Age  int    `gorm:"column:Age"`
}

type ArrayRecord struct {
	Name    string             `gorm:"column:Name"`
	Records []ComplexSubRecord `gorm:"column:Records;type:ARRAY"`
}

type SuperComplexRecord struct {
	Name         string                  `gorm:"column:Name"`
	SuperRecord  SuperComplexSubRecord   `gorm:"column:Record;type:RECORD"`
	SuperRecords []SuperComplexSubRecord `gorm:"column:Records;type:ARRAY"`
}

type SuperComplexSubRecord struct {
	Name   string           `gorm:"column:Name"`
	Record ComplexSubRecord `gorm:"column:Record;type:RECORD"`
}
