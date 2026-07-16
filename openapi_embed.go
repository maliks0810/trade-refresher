package traderefresher

import _ "embed"

// OpenAPISpec is the checked-in BlackRock Trade API specification.
//
//go:embed openapi.json
var OpenAPISpec []byte
