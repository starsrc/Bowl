package migrations

import "embed"

// FS embeds all goose migration SQL files.
//
//go:embed *.sql
var FS embed.FS
