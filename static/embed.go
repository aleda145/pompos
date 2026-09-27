package static

import "embed"

// FS contains public web assets.
//
//go:embed *.css *.js
var FS embed.FS
