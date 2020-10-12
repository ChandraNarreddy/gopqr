package gopqr

import (
	"database/sql/driver"
	"errors"
	"fmt"
	nurl "net/url"
	"sync"

	"github.com/lib/pq"
)

/*
Author: Chandrakanth Narreddy
This is an abstraction over github.com/lib/pq pacakge to add support for
rotating credentials.

Usage:
1. Create the driver like this -
	import go-pqr

	pqrDriver := &go-pqr.Driver{
	...
	}

2. Define a credential refresher function as per your need and supply like -
	pqrDriver.CredentialRefresher = func(pqrDriver *pqr.Driver) {
	...
	pqrDriver.AcquireLock()
	...
	pqrDriver.Rotating = false
	pqrDriver.ReleaseLock()
	return
	}

3. Register postgresrotating driver like this -
	func init() {
		sql.Register("postgresrotating", &Driver{})
	}

4. Zero touch credential rotation. Yay!!!
*/

type rotaterEnum int

const (
	ODD_USER rotaterEnum = iota
	ODD_PASSWORD
	EVEN_USER
	EVEN_PASSWORD
	ACTIVE_CREDENTIAL
	ODD_CREDENTIAL
	EVEN_CREDENTIAL
)

func (d rotaterEnum) String() string {
	return [...]string{"odd_username", "odd_password", "even_username", "even_password", "active_credential", "odd", "even"}[d]
}

type Driver struct {
	Odd_username        string
	Odd_password        string
	Even_username       string
	Even_password       string
	Active_credential   string
	Rotating            bool
	mux                 sync.Mutex
	CredentialRefresher func(*Driver)
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	//parse the odd and even pair from the string and
	//fetch alternating pairs to call pq.Open() here and
	//pass the DSN as "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
	activeDSN, err := d.fetchActive(dsn)
	if err != nil {
		return nil, err
	}
	d.rotateActive()
	conn, connErr := pq.Open(activeDSN)
	if connErr != nil {
		if connErr.(*pq.Error).Code == "28000" || connErr.(*pq.Error).Code == "28P01" {
			rotatedDSN, _ := d.fetchActive(dsn)
			d.setRotating()
			go d.refreshCredentials()
			conn, connErr = pq.Open(rotatedDSN)
			if connErr != nil {
				return nil, errors.New("Both the credentials failed")
			}
			return conn, nil
		}
		return nil, connErr
	}
	return conn, nil
}

func (d *Driver) rotateActive() {
	d.mux.Lock()
	if d.Active_credential == ODD_CREDENTIAL.String() {
		d.Active_credential = EVEN_CREDENTIAL.String()
	} else {
		d.Active_credential = ODD_CREDENTIAL.String()
	}
	d.mux.Unlock()
}

func (d *Driver) setRotating() {
	d.mux.Lock()
	d.Rotating = true
	d.mux.Unlock()
}

func (d *Driver) unsetRotating() {
	d.mux.Lock()
	d.Rotating = false
	d.mux.Unlock()
}

func (d *Driver) refreshCredentials() {
	d.CredentialRefresher(d)
}

func (d *Driver) AcquireLock() {
	d.mux.Lock()
}

func (d *Driver) ReleaseLock() {
	d.mux.Unlock()
}

func (d *Driver) fetchActive(dsn string) (string, error) {
	u, err := nurl.Parse(dsn)
	if err != nil {
		return "", errors.New("Failed while parsing Rotating DSN")
	}
	q := u.Query()
	var activeUser, activePass string

	if d.Active_credential == ODD_CREDENTIAL.String() {
		activeUser = d.Odd_username
		activePass = d.Odd_password
	} else {
		activeUser = d.Even_username
		activePass = d.Even_password
	}
	return fmt.Sprintf("postgres://%v:%v@%v%v?%v", activeUser, activePass, u.Host, u.Path, q.Encode()), nil
}
