package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hacomono-lib/o4x/schema"
)

func main() {
	// Flags
	outboxTable := flag.String("outbox", "outbox", "Outbox table name")
	consumerTable := flag.String("consumer", "consumer_messages", "Consumer messages table name (used with --with-consumer)")
	withConsumer := flag.Bool("with-consumer", false, "Include consumer_messages table DDL")
	rollback := flag.Bool("rollback", false, "Generate rollback (DROP) SQL instead of migration")
	help := flag.Bool("help", false, "Show help")
	flag.BoolVar(help, "h", false, "Show help")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `o4x-schema - Generate DDL for o4x tables

Usage:
  o4x-schema [options]

Options:
`)
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  # Generate outbox table only (default)
  o4x-schema > migration.sql

  # Generate outbox table with custom name
  o4x-schema --outbox my_outbox > migration.sql

  # Generate both outbox and consumer tables
  o4x-schema --with-consumer > migration.sql

  # Generate both with custom names
  o4x-schema --outbox my_outbox --with-consumer --consumer my_consumer_messages > migration.sql

  # Generate rollback SQL
  o4x-schema --rollback > rollback.sql
  o4x-schema --rollback --with-consumer > rollback.sql
`)
	}

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	var output string

	if *rollback {
		if *withConsumer {
			output = schema.RollbackSQL(*outboxTable, *consumerTable)
		} else {
			output = schema.DropOutboxDDL(*outboxTable)
		}
	} else {
		if *withConsumer {
			output = schema.MigrationSQL(*outboxTable, *consumerTable)
		} else {
			output = schema.OutboxDDL(*outboxTable)
		}
	}

	fmt.Print(output)
}
