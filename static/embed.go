package static

import "embed"

// FS contains public web assets.
//
//go:embed *.css
var FS embed.FS
