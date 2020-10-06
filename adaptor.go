package bigquery

import (
	"database/sql/driver"
	"gorm.io/driver/bigquery/adaptor"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"reflect"
)

type bigQuerySchemaAdaptor struct {
	schema *schema.Schema
	db     *gorm.DB
}

func (schemaAdaptor *bigQuerySchemaAdaptor) GetColumnAdaptor(name string) adaptor.SchemaColumnAdaptor {

	if schema := schemaAdaptor.schema; schema != nil {

		field := schema.FieldsByDBName[name]

		if field == nil {
			return nil
		}

		switch field.DataType {
		case adaptor.RecordType, adaptor.ArrayType:
			return &bigQueryColumnAdaptor{field: field, rootDB: schemaAdaptor.db}
		}
	}

	return nil
}

type bigQueryColumnAdaptor struct {
	field  *schema.Field
	rootDB *gorm.DB
}

func (columnAdaptor *bigQueryColumnAdaptor) AdaptValue(value driver.Value) (driver.Value, error) {
	instance := reflect.New(columnAdaptor.field.IndirectFieldType).Interface()

	db := columnAdaptor.rootDB.Raw(adaptor.RerouteQuery, value)

	err := db.Statement.Parse(instance)
	if err != nil {
		return nil, err
	}

	applyStatementSchemaContext(db, columnAdaptor.rootDB)

	err = db.Scan(instance).Error
	if err != nil {
		return nil, err
	}

	return instance, err
}

func (columnAdaptor *bigQueryColumnAdaptor) GetSchemaAdaptor() adaptor.SchemaAdaptor {
	schema := columnAdaptor.field.Schema

	if schema == nil {
		return nil
	}
	return &bigQuerySchemaAdaptor{
		schema: schema,
		db:     columnAdaptor.rootDB,
	}
}
