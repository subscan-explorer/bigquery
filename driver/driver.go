package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type bigQueryDriver struct {
}

type bigQueryConfig struct {
	projectID string
	location  string
	dataSet   string
	scopes    []string
}

func (b bigQueryDriver) Open(uri string) (driver.Conn, error) {

	if uri == "scanner" {
		return &scannerConnection{}, nil
	}

	config, err := configFromUri(uri)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, config.projectID, option.WithScopes(config.scopes...))
	if err != nil {
		return nil, err
	}

	return &bigQueryConnection{
		ctx:    ctx,
		client: client,
		config: *config,
	}, nil
}

func configFromUri(uri string) (*bigQueryConfig, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, invalidConnectionStringError(uri)
	}

	if u.Scheme != "bigquery" {
		return nil, fmt.Errorf("invalid prefix, expected bigquery:// got: %s", uri)
	}

	if u.Path == "" {
		return nil, invalidConnectionStringError(uri)
	}

	fields := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(fields) > 2 {
		return nil, invalidConnectionStringError(uri)
	}

	config := &bigQueryConfig{
		projectID: u.Hostname(),
		dataSet:   fields[len(fields)-1],
		scopes:    getScopes(u.Query()),
	}

	if len(fields) == 2 {
		config.location = fields[0]
	}

	return config, nil
}

func getScopes(query url.Values) []string {
	q := strings.Trim(query.Get("scopes"), ",")
	if q == "" {
		return []string{}
	}
	return strings.Split(q, ",")
}

func invalidConnectionStringError(uri string) error {
	return fmt.Errorf("invalid connection string: %s", uri)
}
