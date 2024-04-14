# keyspace

keyspace is a session manager for aws keyspace and gocql.

## Installation

```bash
go get github.com/snowmerak/keyspace
```

## Usage

```go
package main

func main() {
    cl := gocql.NewCluster("<your keyspace endpoint>")
    cl.Keyspace = "<your keyspace name>"
    
    ks, err := keyspace.New(cl)
    if err != nil {
        panic(err)
    }
    defer ks.Close()

    if err := Query(func (s *gocql.Session) error {
        return s.Query("SELECT * FROM <your table>").Exec()
    }); err != nil {
        panic(err)
    }
}