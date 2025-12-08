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
	inboxTable := flag.String("inbox", "consumer_inbox", "Consumer inbox table name (used with --with-inbox)")
	withInbox := flag.Bool("with-inbox", false, "Include consumer_inbox table DDL (Transactional Inbox pattern)")
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

  # Generate outbox + consumer_inbox (Transactional Inbox pattern - recommended)
  o4x-schema --with-inbox > migration.sql

  # Custom table names
  o4x-schema --outbox my_outbox --with-inbox --inbox my_inbox > migration.sql

  # Generate rollback SQL
  o4x-schema --rollback > rollback.sql
  o4x-schema --rollback --with-inbox > rollback.sql
`)
	}

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	var output string

	if *rollback {
		// Generate rollback (DROP) SQL
		output = schema.DropOutboxDDL(*outboxTable)
		if *withInbox {
			output += "\n" + schema.DropConsumerInboxDDL(*inboxTable)
		}
	} else {
		// Generate migration (CREATE) SQL
		output = schema.OutboxDDL(*outboxTable)
		if *withInbox {
			output += "\n" + schema.ConsumerInboxDDL(*inboxTable)
		}
	}

	fmt.Print(output)
}
