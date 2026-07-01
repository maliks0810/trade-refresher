package net

import (
	"fmt"
)

// ConnectionURLBuilder func for building URL connection.
func ConnectionURLBuilder(n string) (string, error) {
	var url string

	switch n {
	case "fiber":
		// URL for Fiber connection.
		url = fmt.Sprintf(
			":%s",
			"8100",
		)
	default:
		// Return error message.
		return "", fmt.Errorf("connection name '%v' is not supported", n)
	}

	// Return connection URL.
	return url, nil
}