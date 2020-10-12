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
	import gopqr

	pqrDriver := &gopqr.Driver{
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

4. Open the DB with the newly registered driver like this -
	db, err := sqlx.Open("postgresrotating", "postgres://1.2.3.4:5432/mydb?sslmode=verify-full")
	Please donot pass any credentials in the dsn string above.

5. Zero touch credential rotation. Yay!!!
*/

type rotaterEnum int

const (
	oddUser rotaterEnum = iota
	oddPassword
	evenUser
	evenPassword
	activeCredential
	oddCredential
	evenCredential
)

func (d rotaterEnum) String() string {
	return [...]string{"odd_username", "odd_password", "even_username", "even_password", "active_credential", "odd", "even"}[d]
}

// Driver represents a lib/pq compliant driver for rotating credentials.
// It allows you to define an alternating set of credentials for your postgres
// connections. The credentials can be thought of as an odd and even credential
// set that are employed alternatively. During such alternations, if one of the
// credentials results in an authentication failure, the driver falls back to
// make the connection using the previous credential while asynchronously invoking
// the CredentialsRefresher func defined within this driver to refresh both the
// credentials.
type Driver struct {
	// OddUsername - Username for the odd credential
	OddUsername string
	// OddPassword - Password value for the odd credential
	OddPassword string
	// EvenUsername - Username for the even credential
	EvenUsername string
	// EvenPassword - Password value for the even credential
	EvenPassword string
	// ActiveCredential - Which one you wish as first active credential - "odd"/"even"
	ActiveCredential string
	mux              sync.Mutex
	// CredentialRefresher func is what refreshes the credentials set and assigns
	// refreshed values to Odd and even Usernames and Passwords. Please make sure
	// that the function goes in these lines -
	// func(d *gopqr.Driver) {
	//		...logic to refresh the credential values odd and even
	//		d.AcquireLock()
	//		d.OddUsername = ..the value you fetched above..
	//		d.OddPassword = ..the value you fetched above..
	//		d.EvenUsername = ..the value you fetched above..
	//		d.EvenPassword = ..the value you fetched above..
	//		d.ActiveCredential = ..the value you fetched above..
	//		d.ReleaseLock()
	//		return
	// }
	CredentialRefresher func(*Driver)
}

// Open does the same thing as pq.Open() except that it uses the gopqr driver.
// Please ensure to pass the DSN as "postgres://1.2.3.4:5432/mydb?sslmode=mode"
// to your sql.Open() or sqlx.Open() implementations.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	// parses the odd and even pair from the string and
	// fetches alternating pairs to call pq.Open() here and
	// passes the DSN as "postgres://user_name:password@1.2.3.4:5432/mydb?sslmode=verify-full"
	// to the underlying pq handler
	activeDSN, err := d.fetchActive(dsn)
	if err != nil {
		return nil, err
	}
	d.rotateActive()
	conn, connErr := pq.Open(activeDSN)
	if connErr != nil {
		if connErr.(*pq.Error).Code == "28000" || connErr.(*pq.Error).Code == "28P01" {
			rotatedDSN, _ := d.fetchActive(dsn)
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
	if d.ActiveCredential == oddCredential.String() {
		d.ActiveCredential = evenCredential.String()
	} else {
		d.ActiveCredential = oddCredential.String()
	}
	d.mux.Unlock()
}

func (d *Driver) refreshCredentials() {
	d.CredentialRefresher(d)
}

// AcquireLock acquires a lock on the driver object
func (d *Driver) AcquireLock() {
	d.mux.Lock()
}

// ReleaseLock releases any lock acquired on the driver object
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

	if d.ActiveCredential == oddCredential.String() {
		activeUser = d.OddUsername
		activePass = d.OddPassword
	} else {
		activeUser = d.EvenUsername
		activePass = d.EvenPassword
	}
	return fmt.Sprintf("postgres://%v:%v@%v%v?%v", activeUser, activePass, u.Host, u.Path, q.Encode()), nil
}
