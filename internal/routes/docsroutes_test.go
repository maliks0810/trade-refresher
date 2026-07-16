package routes

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestDocsRoutesServeSwaggerUIAndProxySpec(t *testing.T) {
	app := fiber.New()
	if err := DocsRoutes(app, true); err != nil {
		t.Fatalf("DocsRoutes returned error: %v", err)
	}

	response, err := app.Test(httptest.NewRequest(http.MethodGet, apiBasePath+"/docs/index.html", nil))
	if err != nil {
		t.Fatalf("request Swagger UI: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read Swagger UI: %v", err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "Aladdin Trade Refresher API") {
		t.Fatalf("Swagger UI status = %d, expected rendered page", response.StatusCode)
	}

	response, err = app.Test(httptest.NewRequest(http.MethodGet, apiBasePath+"/openapi.json", nil))
	if err != nil {
		t.Fatalf("request OpenAPI specification: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("OpenAPI status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var document map[string]any
	if err := json.NewDecoder(response.Body).Decode(&document); err != nil {
		t.Fatalf("decode OpenAPI specification: %v", err)
	}
	assertProxyOpenAPI(t, document)
}

func TestDocsRoutesAreNotRegisteredWhenDisabled(t *testing.T) {
	app := fiber.New()
	if err := DocsRoutes(app, false); err != nil {
		t.Fatalf("DocsRoutes returned error: %v", err)
	}
	for _, path := range []string{apiBasePath + "/docs/", apiBasePath + "/openapi.json"} {
		response, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil))
		if err != nil {
			t.Fatalf("request %s: %v", path, err)
		}
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d", path, response.StatusCode, http.StatusNotFound)
		}
	}
}

func assertProxyOpenAPI(t *testing.T, document map[string]any) {
	t.Helper()
	servers := document["servers"].([]any)
	if got := servers[0].(map[string]any)["url"]; got != apiBasePath {
		t.Fatalf("server URL = %v, want %s", got, apiBasePath)
	}
	components := document["components"].(map[string]any)
	securitySchemes := components["securitySchemes"].(map[string]any)
	if _, ok := securitySchemes["BearerAuth"]; !ok {
		t.Fatal("BearerAuth security scheme is missing")
	}
	security := document["security"].([]any)
	if len(security) != 1 {
		t.Fatalf("global security requirement count = %d, want 1", len(security))
	}

	paths := document["paths"].(map[string]any)
	if len(paths) != 7 {
		t.Fatalf("path count = %d, want 7", len(paths))
	}
	for path, pathValue := range paths {
		for method, operationValue := range pathValue.(map[string]any) {
			if method != "get" && method != "post" {
				continue
			}
			parameters := operationValue.(map[string]any)["parameters"].([]any)
			foundCorrelationID := false
			for _, parameterValue := range parameters {
				parameter := parameterValue.(map[string]any)
				name, _ := parameter["name"].(string)
				if strings.HasPrefix(name, "VND.com.blackrock.") {
					t.Fatalf("%s %s exposes generated upstream header %s", method, path, name)
				}
				if name == "X-Correlation-ID" && parameter["required"] == true {
					foundCorrelationID = true
				}
			}
			if !foundCorrelationID {
				t.Fatalf("%s %s does not require X-Correlation-ID", method, path)
			}
			description, _ := operationValue.(map[string]any)["description"].(string)
			if !strings.Contains(description, "Authorization: Bearer") || !strings.Contains(description, "X-Correlation-ID") {
				t.Fatalf("%s %s does not explain required refresher headers", method, path)
			}
			responses := operationValue.(map[string]any)["responses"].(map[string]any)
			for _, status := range []string{"200", "400", "401", "502", "503"} {
				if _, ok := responses[status]; !ok {
					t.Fatalf("%s %s is missing documented %s response", method, path, status)
				}
			}
		}
	}
}
