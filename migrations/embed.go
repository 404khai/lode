// Package migrations embeds Ledger's Postgres schema migrations.
// Files are named NNNNN_description.sql and are never edited once merged.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
