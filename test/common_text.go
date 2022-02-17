package test

import (
	"log"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/bigquery"
	"gorm.io/gorm"
)

type GormTestSuite struct {
	suite.Suite
	db *gorm.DB
}

func (suite *GormTestSuite) SetupSuite() {

	logrus.SetLevel(logrus.DebugLevel)

	var err error
	suite.db, err = gorm.Open(bigquery.Open("bigquery://go-bigquery-driver/playground"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
}

func (suite *GormTestSuite) TearDownSuite() {

}
