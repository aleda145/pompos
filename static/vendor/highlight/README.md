highlight.js 11.11.1, BSD-3-Clause (see LICENSE).

Upstream: https://github.com/highlightjs/highlight.js/tree/11.11.1

Vendored so syntax highlighting works without a runtime CDN dependency. This
bundle includes only the core, Python, and YAML. Token colors live in static/style.css.

To rebuild from an installed highlight.js 11.11.1 package:

    node static/vendor/highlight/build.cjs /path/to/node_modules/highlight.js
