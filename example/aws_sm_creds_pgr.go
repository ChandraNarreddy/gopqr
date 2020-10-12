package example

import (
	"github.com/chandranarreddy/gopqr"

	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
)

/*
Author: Chandrakanth Narreddy
This is an example usage of github.com/chandranarredddy/gopqr for zero-touch
rotating credentials for postgres connections when credentials are refreshed
from AWS Secrets Manager. This example assumes that the credentials in Secrets
Manager are in the following json format -
	{
		"odd_username": "myOddUserName",
		"odd_password": "myOddPassword",
		"even_username": "myEvenUserName",
		"even_password": "myEvenPassword",
		"active_credential": "even", // or could be "odd"
	}
*/

const (
	//AWSREGION is the aws region where your postgres credentials are stored
	AWSREGION = "us-west-2"
	//SECRETENTRY is the entry name where credentials are stored
	SECRETENTRY = "mysecretmanagerentry"
	//DEFAULTSETMAXOPENCONNS - default for max open connections
	DEFAULTSETMAXOPENCONNS = 5
	//DEFAULTSETMAXIDLECONNS - default for max idle connections in the pool
	DEFAULTSETMAXIDLECONNS = 2
	//DEFAULTSETCONNMAXLIFETIMEINHOURS - default for max lifetime of each connection.
	//After the expiry of this window is when the rotation of the active credential happens
	DEFAULTSETCONNMAXLIFETIMEINHOURS = 24
	//DEFAULTSSLMODE - default for SSL Mode setting
	DEFAULTSSLMODE = "require"

	//DBNAME - change this to your db's name
	DBNAME = "mypostgres"
	//DBADDR - change this to your db's address
	DBADDR = "localhost"
)

//NewRotatingCredentialsDB returns the rotating credentials postgres connection.
func NewRotatingCredentialsDB(logger *log.Logger) (*sqlx.DB, error) {
	pqrDriver, buildPQRDriverErr := buildPQRDriver(logger)
	if buildPQRDriverErr != nil {
		logger.Print(buildPQRDriverErr)
		return nil, fmt.Errorf("failed to build PQR Driver - %v", buildPQRDriverErr)
	}
	sql.Register("postgresrotating", pqrDriver)
	dsn := fmt.Sprintf("postgres://%v/%v?sslmode=%v", DBADDR, DBNAME, DEFAULTSSLMODE)
	db, err := sqlx.Open("postgresrotating", dsn)
	if err != nil {
		logger.Print(fmt.Errorf("failed to create DB - %v", err))
		return nil, fmt.Errorf("failed to create DB - %v", err)
	}
	db.SetMaxOpenConns(DEFAULTSETMAXOPENCONNS)
	db.SetMaxIdleConns(DEFAULTSETMAXIDLECONNS)
	db.SetConnMaxLifetime(time.Hour * DEFAULTSETCONNMAXLIFETIMEINHOURS)
	return db, nil
}

func buildPQRDriver(logger *log.Logger) (*gopqr.Driver, error) {
	sm := secretsmanager.New(newAWSSession(AWSREGION))
	secretInput := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(SECRETENTRY),
		VersionStage: aws.String("AWSCURRENT"),
	}
	result, err := sm.GetSecretValue(secretInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case secretsmanager.ErrCodeResourceNotFoundException:
				logger.Print(errors.New(secretsmanager.ErrCodeResourceNotFoundException))
				return nil, errors.New(secretsmanager.ErrCodeResourceNotFoundException)
			case secretsmanager.ErrCodeInvalidParameterException:
				logger.Print(errors.New(secretsmanager.ErrCodeInvalidParameterException))
				return nil, errors.New(secretsmanager.ErrCodeInvalidParameterException)
			case secretsmanager.ErrCodeInvalidRequestException:
				logger.Print(errors.New(secretsmanager.ErrCodeInvalidRequestException))
				return nil, errors.New(secretsmanager.ErrCodeInvalidRequestException)
			case secretsmanager.ErrCodeDecryptionFailure:
				logger.Print(errors.New(secretsmanager.ErrCodeDecryptionFailure))
				return nil, errors.New(secretsmanager.ErrCodeDecryptionFailure)
			case secretsmanager.ErrCodeInternalServiceError:
				logger.Print(errors.New(secretsmanager.ErrCodeInternalServiceError))
				return nil, errors.New(secretsmanager.ErrCodeInternalServiceError)
			default:
				logger.Print(errors.New(aerr.Error()))
				return nil, errors.New(aerr.Error())
			}
		}
		logger.Print(err.(awserr.Error))
		return nil, err.(awserr.Error)
	}
	var s struct {
		OddUsername      string `json:"odd_username"`
		OddPassword      string `json:"odd_password"`
		EvenUsername     string `json:"even_username"`
		EvenPassword     string `json:"even_password"`
		ActiveCredential string `json:"active_credential"`
	}

	err = json.Unmarshal([]byte(*result.SecretString), &s)
	if err != nil {
		logger.Print(fmt.Errorf("Unmarshalling secret failed while fetching DB secret from SM - %v", err))
		return nil, fmt.Errorf("Unmarshalling secret failed while fetching DB secret from SM - %v", err)
	}
	pqrDriver := &gopqr.Driver{
		OddUsername:      s.OddUsername,
		OddPassword:      s.OddPassword,
		EvenUsername:     s.EvenUsername,
		EvenPassword:     s.EvenPassword,
		ActiveCredential: s.ActiveCredential,
		Rotating:         false,
	}
	pqrDriver.CredentialRefresher = func(pqrDriver *gopqr.Driver) {
		secretInput := &secretsmanager.GetSecretValueInput{
			SecretId:     aws.String(SECRETENTRY),
			VersionStage: aws.String("AWSCURRENT"),
		}
		sm := secretsmanager.New(newAWSSession(AWSREGION))
		result, err := sm.GetSecretValue(secretInput)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case secretsmanager.ErrCodeResourceNotFoundException:
					logger.Print(errors.New(secretsmanager.ErrCodeResourceNotFoundException))
				case secretsmanager.ErrCodeInvalidParameterException:
					logger.Print(errors.New(secretsmanager.ErrCodeInvalidParameterException))
				case secretsmanager.ErrCodeInvalidRequestException:
					logger.Print(errors.New(secretsmanager.ErrCodeInvalidRequestException))
				case secretsmanager.ErrCodeDecryptionFailure:
					logger.Print(errors.New(secretsmanager.ErrCodeDecryptionFailure))
				case secretsmanager.ErrCodeInternalServiceError:
					logger.Print(errors.New(secretsmanager.ErrCodeInternalServiceError))
				default:
					logger.Print(errors.New(aerr.Error()))
				}
				return
			}
			logger.Print(err.(awserr.Error))
			return
		}
		var s struct {
			OddUsername      string `json:"odd_username"`
			OddPassword      string `json:"odd_password"`
			EvenUsername     string `json:"even_username"`
			EvenPassword     string `json:"even_password"`
			ActiveCredential string `json:"active_credential"`
		}
		err = json.Unmarshal([]byte(*result.SecretString), &s)
		fmt.Printf("unmarshalled secretentry - %#v", s)
		if err != nil {
			logger.Print(fmt.Errorf("Unmarshalling secret failed while refreshing DB secret from SM - %v", err))
			return
		}
		pqrDriver.AcquireLock()
		pqrDriver.OddUsername = s.OddUsername
		pqrDriver.OddPassword = s.OddPassword
		pqrDriver.EvenUsername = s.EvenUsername
		pqrDriver.EvenPassword = s.EvenPassword
		pqrDriver.ActiveCredential = s.ActiveCredential
		pqrDriver.Rotating = false
		pqrDriver.ReleaseLock()
		return
	}
	return pqrDriver, nil

}

func newAWSSession(region string) *session.Session {
	sess := session.New()
	creds := credentials.NewCredentials(&ec2rolecreds.EC2RoleProvider{
		Client:       ec2metadata.New(sess),
		ExpiryWindow: 5 * time.Minute,
	})
	awsConfig := &aws.Config{
		Credentials: creds,
		Region:      aws.String(region),
	}
	return session.New(awsConfig)
}
