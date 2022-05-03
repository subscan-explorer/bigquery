package driver

import (
	"cloud.google.com/go/bigquery"
	"errors"
	"gorm.io/driver/bigquery/adaptor"
	"io"
)

type bigQuerySource interface {
	GetSchema() bigQuerySchema
	Next() ([]bigquery.Value, error)
}

type bigQueryRowIteratorSource struct {
	iterator      *bigquery.RowIterator
	schemaAdaptor adaptor.SchemaAdaptor
	prevValues []bigquery.Value
	prevError error
}

func (source *bigQueryRowIteratorSource) GetSchema() bigQuerySchema {
	return createBigQuerySchema(source.iterator.Schema, source.schemaAdaptor)
}

func (source *bigQueryRowIteratorSource) Next() ([]bigquery.Value, error) {
	var values []bigquery.Value
	var err error
	if source.prevValues != nil || source.prevError != nil {
		values = source.prevValues
		err = source.prevError
		source.prevValues = nil
		source.prevError = nil
	} else {
		err = source.iterator.Next(&values)
	}
	return values, err
}

func createSourceFromRowIterator(rowIterator *bigquery.RowIterator, schemaAdaptor adaptor.SchemaAdaptor) bigQuerySource {
	source := &bigQueryRowIteratorSource{
		iterator:      rowIterator,
		schemaAdaptor: schemaAdaptor,
	}
	// Call RowIterator.Next once so that calls to source.iterator.Schema will return values
	if source.iterator != nil {
		source.prevError = source.iterator.Next(&source.prevValues)
	}
	return source
}

type bigQueryColumnSource struct {
	schema   bigQuerySchema
	rows     []bigquery.Value
	position int
}

func (source *bigQueryColumnSource) GetSchema() bigQuerySchema {
	return source.schema
}

func (source *bigQueryColumnSource) Next() ([]bigquery.Value, error) {
	if source.position >= len(source.rows) {
		return nil, io.EOF
	}
	values, ok := source.rows[source.position].([]bigquery.Value)
	if !ok {
		return nil, errors.New("failed to get row from column source")
	}
	source.position++
	return values, nil
}

func createSourceFromColumn(schema bigQuerySchema, rows []bigquery.Value) bigQuerySource {
	return &bigQueryColumnSource{
		schema:   schema,
		rows:     rows,
		position: 0,
	}
}
