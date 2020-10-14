# gopqr
Golang handler for zero-touch rotating credentials Postgres Connections. Use this wrapper for lib/pq driver to facilitate zero-touch rotating credentials connections for your applications.

## Zero Touch Credentials Rotation
How many times have we wished that credentials were refreshed without needing to take a hit on your application servers. This quick and simple utility gopqr solves the problem if you have a Golang based application connecting using lib/pq to a Postgres database.

## How does it work?
gopqr driver works by alternating between odd and even pairs of credentials for each subsequent database connection it makes. So, when credentials to one of the odd/even accounts changes, the driver would encounter 'Credential failure' code when attempting to make a connection using already expired credentials of this account. This is when the driver falls back to the still-good account to continue making the database connection and asynchronously invokes the 'CredentialRefresher' function t
o refresh the credentials set, thereby avoiding a down-time. The 'CredentialRefresher' function is defined by the consumer of gopqr(you), when invoked it should fetch the latest set of credentials from storage such as a vault and reset the odd and even credentials in the driver. Make sure you do this within the protection of the inbuilt Mutex to avoid race conditions.

## Okay, how do I make it happen?
Please refer to the [sample](https://github.com/ChandraNarreddy/gopqr/blob/main/example/aws_sm_creds_pgr.go) code in the examples directory for usage of the driver by refreshing credentials stored in AWS Secrets Manager.

### Instructions
* If you have defined your postgres database service accounts to refresh every often, you can use this little utility to automatically refresh these credentials for you. Only requirement is that you need to have 2 such service accounts with similar privilege level for the sake of continuity while the other one is under rotation. Once you have created the second account, you are good to go!

* Here is how you would create the driver -

```
  pqr := &gopqr.Driver{
      OddUsername:      s.OddUsername,
      OddPassword:      s.OddPassword,
      EvenUsername:     s.EvenUsername,
      EvenPassword:     s.EvenPassword,
      ActiveCredential: s.ActiveCredential,
    }
```

* You will need to define a function that fetches the latest set of credentials from where they are stored such as from a vault or AWS Secrets Manager. This function is invoked by the driver upon encountering 'incorrect credentials' error when negotiating a new connection to the database.
```
  pqr.CredentialRefresher = func(pqrDriver *gopqr.Driver) {
    ..
    fetch the new credentials
    ...
    pqrDriver.AcquireLock()
    pqrDriver.OddUsername = `odd credential username fetched above`
    pqrDriver.OddPassword = `odd credential password fetched above`
    pqrDriver.EvenUsername = `even credential username fetched above`
    pqrDriver.EvenPassword = `even credential password fetched above`
    pqrDriver.ActiveCredential = `odd/even`
    pqrDriver.ReleaseLock()
    return
  }
```
* Now register the newly minted driver like this -
```
  sql.Register("postgresrotating", pqrDriver)
```
* Create the database dsn sans the credentials like this -
```
  dsn := fmt.Sprintf("postgres://%v/%v?sslmode=%v", MyDBAddr, MyDBName, 'require')
```
* Now, open the connection to the DB using the SQL implementation of your choice -
```
  db, err := sqlx.Open("postgresrotating", dsn)
```
* You can use the SetConnMaxLifetime parameter to set when the connections timeout, this setting defines how frequently the database connections are alternated between the odd and the even credential.
```
  db.SetConnMaxLifetime(time.Hour * MaxLifetimeInHours)
```
* When you rotate credentials for these accounts, remember to space them apart in time (greater than one multiple of SetConnMaxLifetime value above) so the driver does not end up with credentials invalid for both accounts when it attempts to make a connection to the database at the end of a lifetime window.

* You can get creative with the CredentialRefresher function to introduce alerting capabilities in case the function fails to accurately refresh the credentials.

## Contributions
Please feel free to send a PR or raise an issue.

## Author
Chandrakanth Narreddy

## License
MIT License
