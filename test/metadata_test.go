package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type MetadataTestSuit struct {
	GormTestSuite
}

func TestMetadataTestSuit(t *testing.T) {
	suite.Run(t, new(MetadataTestSuit))
}

func (suite *MetadataTestSuit) Test_HasTable() {
	assert.False(suite.T(), suite.db.Migrator().HasTable("non_existing_table"))
}

func (suite *MetadataTestSuit) Test_CRUDTableWithNoStructsAndArrays() {
	suite.db.Migrator().DropTable(&SimpleTestRecord{})
	assert.False(suite.T(), suite.db.Migrator().HasTable(&SimpleTestRecord{}))
	suite.db.AutoMigrate(&SimpleTestRecord{})
	assert.True(suite.T(), suite.db.Migrator().HasTable(&SimpleTestRecord{}))
	suite.db.Create(&SimpleTestRecord{Name: "test"})

	var records []SimpleTestRecord
	suite.db.First(&records)

	assert.Equal(suite.T(), 1, len(records), "should be a records")
	if len(records) > 0 {
		assert.Equal(suite.T(), "test", records[0].Name)
	}
}

func (suite *MetadataTestSuit) Test_CRUDTableWithStruct() {
	var records []ComplexRecord
	suite.db.Migrator().DropTable(&ComplexRecord{})
	assert.False(suite.T(), suite.db.Migrator().HasTable(&ComplexRecord{}))
	suite.db.AutoMigrate(&ComplexRecord{})
	assert.True(suite.T(), suite.db.Migrator().HasTable(&ComplexRecord{}))
	suite.db.Create(&ComplexRecord{Name: "test", Record: ComplexSubRecord{Name: "dd", Age: 1}})
	suite.db.Create(&ComplexRecord{Name: "test2", Record: ComplexSubRecord{Name: "dd2", Age: 444}})
	suite.db.Order("Name").Find(&records)
	assert.Equal(suite.T(), 2, len(records), "we should have two records")
	if len(records) == 2 {
		assert.Equal(suite.T(), 444, records[1].Record.Age)
	}
}

func (suite *MetadataTestSuit) Test_CRUDTableWithArray() {
	var records []ArrayRecord
	suite.db.Migrator().DropTable(&ArrayRecord{})
	assert.False(suite.T(), suite.db.Migrator().HasTable(&ArrayRecord{}))
	suite.db.AutoMigrate(&ArrayRecord{})
	assert.True(suite.T(), suite.db.Migrator().HasTable(&ArrayRecord{}))
	suite.db.Create(&ArrayRecord{Name: "test", Records: []ComplexSubRecord{{Name: "dd", Age: 1}, {Name: "dd1", Age: 1}}})
	suite.db.Create(&ArrayRecord{Name: "test2", Records: []ComplexSubRecord{{Name: "dd2", Age: 444}, {Name: "dd3", Age: 1}}})
	suite.db.Order("Name").Find(&records)
	assert.Equal(suite.T(), 2, len(records), "we should have two records")
	if len(records) == 2 {
		assert.Equal(suite.T(), 444, records[1].Records[0].Age)
	}
}

func (suite *MetadataTestSuit) Test_CRUDTableWithMultipleNesting() {
	var records []SuperComplexRecord
	suite.db.Migrator().DropTable(&SuperComplexRecord{})
	assert.False(suite.T(), suite.db.Migrator().HasTable(&SuperComplexRecord{}))
	suite.db.AutoMigrate(&SuperComplexRecord{})
	assert.True(suite.T(), suite.db.Migrator().HasTable(&SuperComplexRecord{}))
	suite.db.Create(&SuperComplexRecord{Name: "test",
		SuperRecord: SuperComplexSubRecord{Name: "rec1", Record: ComplexSubRecord{Name: "name1", Age: 441}},
		SuperRecords: []SuperComplexSubRecord{
			{Name: "sub1", Record: ComplexSubRecord{Name: "dep1", Age: 444}},
			{Name: "sub2", Record: ComplexSubRecord{Name: "dep2", Age: 442}}},
	})
	suite.db.Create(&SuperComplexRecord{Name: "test2",
		SuperRecord: SuperComplexSubRecord{Name: "rec2", Record: ComplexSubRecord{Name: "dep3", Age: 443}},
		SuperRecords: []SuperComplexSubRecord{
			{Name: "sub3", Record: ComplexSubRecord{Name: "dep3", Age: 445}},
			{Name: "sub4", Record: ComplexSubRecord{Name: "dep4", Age: 446}}},
	})
	suite.db.Order("Name").Find(&records)
	assert.Equal(suite.T(), 2, len(records), "we should have two records")
	if len(records) == 2 {
		assert.Equal(suite.T(), 444, records[0].SuperRecords[0].Record.Age)
	}
}
