package static

import "embed"

// FS contains public web assets.
//
//go:embed *.css *.js vendor
var FS embed.FS
