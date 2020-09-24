package driver

import (
	"cloud.google.com/go/bigquery"
	"database/sql/driver"
	"gorm.io/driver/bigquery/adaptor"
)

type bigQuerySchema interface {
	ColumnNames() []string
	ConvertColumnValue(index int, value bigquery.Value) driver.Value
}

type bigQueryColumns struct {
	names   []string
	columns []bigQueryColumn
}

func (columns bigQueryColumns) ConvertColumnValue(index int, value bigquery.Value) driver.Value {
	if index > -1 && len(columns.columns) > index {
		column := columns.columns[index]
		return column.ConvertValue(value)
	}

	return value
}

func (columns bigQueryColumns) ColumnNames() []string {
	return columns.names
}

type bigQueryReroutedColumn struct {
	values []bigquery.Value
	schema bigquery.Schema
}

type bigQueryColumn struct {
	Name    string
	Schema  bigquery.Schema
	Adaptor adaptor.SchemaColumnAdaptor
}

func (column bigQueryColumn) ConvertValue(value bigquery.Value) driver.Value {

	if len(column.Schema) == 0 {
		return value
	}

	values, ok := value.([]bigquery.Value)
	if ok {

		if len(values) > 0 {
			if _, isRows := values[0].([]bigquery.Value); !isRows {
				values = []bigquery.Value{values}
			}
		}

		value = bigQueryReroutedColumn{values: values, schema: column.Schema}
	}

	if columnAdaptor := column.Adaptor; columnAdaptor != nil {
		return columnAdaptor.AdaptValue(value)
	}

	return value
}

func createBigQuerySchema(schema bigquery.Schema, schemaAdaptor adaptor.SchemaAdaptor) bigQuerySchema {
	var names []string
	var columns []bigQueryColumn
	for _, column := range schema {

		name := column.Name

		var columnAdaptor adaptor.SchemaColumnAdaptor
		if schemaAdaptor != nil {
			columnAdaptor = schemaAdaptor.GetColumnAdaptor(name)
		}

		names = append(names, name)
		columns = append(columns, bigQueryColumn{
			Name:    name,
			Schema:  column.Schema,
			Adaptor: columnAdaptor,
		})
	}
	return &bigQueryColumns{
		names,
		columns,
	}
}
