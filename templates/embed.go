package templates

import "embed"

// FS contains the server-rendered page templates.
//
//go:embed *.html
var FS embed.FS
