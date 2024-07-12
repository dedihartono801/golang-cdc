package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func main() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "****",
	}
	syncer := replication.NewBinlogSyncer(cfg)

	// Start sync with specified binlog file and position
	streamer, err := syncer.StartSync(mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  976,
	})
	if err != nil {
		panic(err) // Handle error appropriately
	}

	// Loop to fetch events
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := streamer.GetEvent(ctx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				// Handle timeout
				continue
			}
			panic(err) // Handle other errors appropriately
		}

		//ev.Dump(os.Stdout)

		// Process the event based on its type
		switch e := ev.Event.(type) {
		case *replication.RowsEvent:
			// Handle RowsEvent (insert, update, delete)
			fmt.Printf("Schema: %s, Table: %s\n", e.Table.Schema, e.Table.Table)

			// Print the values
			for _, value := range e.Rows {
				fmt.Println("Values:")
				fmt.Println("ID", value[0])
				for _, v := range value {
					fmt.Printf("%s\n", v)
				}
			}

		case *replication.QueryEvent:
			// Handle QueryEvent (schema changes, etc.)
			fmt.Printf("Schema: %s, Query: %s\n", e.Schema, e.Query)
		}

	}
}
