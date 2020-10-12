package example

import (
	pqr "github.com/chandranarreddy/go-pqr"

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

const (
	DEFAULTSETMAXOPENCONNS           = 5
	DEFAULTSETMAXIDLECONNS           = 2
	DEFAULTSETCONNMAXLIFETIMEINHOURS = 24
	DEFAULTSSLMODE                   = "require"

	DB_NAME = "mypostgres" //change this to your db's name
	DB_ADDR = "localhost"  // change this to your db's address
)

func NewDB(dbType string, cfg *config.BarricadeConfig, logger *log.Logger) (*sqlx.DB, error) {
	pqrDriver, buildPQRDriverErr := buildPQRDriver(logger)
	if buildPQRDriverErr != nil {
		logger.Print(buildPQRDriverErr)
		return nil, fmt.Errorf("failed to build PQR Driver - %v", buildPQRDriverErr)
	}
	sql.Register(postgresrotating.String(), pqrDriver)
	dsn := fmt.Sprintf("postgres://%v/%v?sslmode=%v", DB_ADDR, DB_NAME, DEFAULTSSLMODE)
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

func buildPQRDriver(logger *log.Logger) (*pqr.Driver, error) {
	sm := secretsmanager.New(NewAWSSession(AWSREGION))
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
		Odd_username      string `json:"odd_username"`
		Odd_password      string `json:"odd_password"`
		Even_username     string `json:"even_username"`
		Even_password     string `json:"even_password"`
		Active_credential string `json:"active_credential"`
	}

	err = json.Unmarshal([]byte(*result.SecretString), &s)
	if err != nil {
		logger.Print(fmt.Errorf("Unmarshalling secret failed while fetching DB secret from SM - %v", err))
		return nil, fmt.Errorf("Unmarshalling secret failed while fetching DB secret from SM - %v", err)
	}
	pqrDriver := pqr.Driver{
		Odd_username:      s.Odd_username,
		Odd_password:      s.Odd_password,
		Even_username:     s.Even_username,
		Even_password:     s.Even_password,
		Active_credential: s.Active_credential,
		Rotating:          false,
	}
	pqrDriver.CredentialRefresher = func(pqrDriver *pqr.Driver) {
		secretInput := &secretsmanager.GetSecretValueInput{
			SecretId:     aws.String(SECRETENTRY),
			VersionStage: aws.String("AWSCURRENT"),
		}
		sm := secretsmanager.New(NewAWSSession(SECRETREGION))
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
			Odd_username      string `json:"odd_username"`
			Odd_password      string `json:"odd_password"`
			Even_username     string `json:"even_username"`
			Even_password     string `json:"even_password"`
			Active_credential string `json:"active_credential"`
		}
		err = json.Unmarshal([]byte(*result.SecretString), &s)
		fmt.Printf("unmarshalled secretentry - %#v", s)
		if err != nil {
			logger.Print(fmt.Errorf("Unmarshalling secret failed while refreshing DB secret from SM - %v", err))
			return
		}
		pqrDriver.AcquireLock()
		pqrDriver.Odd_username = s.Odd_username
		pqrDriver.Odd_password = s.Odd_password
		pqrDriver.Even_username = s.Even_username
		pqrDriver.Even_password = s.Even_password
		pqrDriver.Active_credential = s.Active_credential
		pqrDriver.Rotating = false
		pqrDriver.ReleaseLock()
		return
	}
	return pqrDriver, nil

}

func NewAWSSession(region string) *session.Session {
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
