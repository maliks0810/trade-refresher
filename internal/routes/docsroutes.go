package routes

import (
	"encoding/json"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/swagger"

	traderefresher "refresher/trade-refresher"
)

const correlationIDMaxLength = 256

func DocsRoutes(app *fiber.App, enabled bool) error {
	if !enabled {
		return nil
	}
	spec, err := proxyOpenAPISpec()
	if err != nil {
		return err
	}

	app.Get(apiBasePath+"/openapi.json", func(ctx *fiber.Ctx) error {
		ctx.Set(fiber.HeaderCacheControl, "no-store")
		return ctx.Type("json").Send(spec)
	})
	app.Get(apiBasePath+"/docs", func(ctx *fiber.Ctx) error {
		return ctx.Redirect(apiBasePath+"/docs/", fiber.StatusMovedPermanently)
	})
	app.Get(apiBasePath+"/docs/*", swagger.New(swagger.Config{
		Title:                    "Aladdin Trade Refresher API",
		URL:                      apiBasePath + "/openapi.json",
		DeepLinking:              true,
		DisplayOperationId:       true,
		DisplayRequestDuration:   true,
		DocExpansion:             "list",
		DefaultModelsExpandDepth: 1,
		TryItOutEnabled:          true,
		RequestSnippetsEnabled:   true,
		PersistAuthorization:     false,
		ValidatorUrl:             "none",
		SupportedSubmitMethods:   []string{"get", "post"},
	}))
	return nil
}

func proxyOpenAPISpec() ([]byte, error) {
	var document map[string]any
	if err := json.Unmarshal(traderefresher.OpenAPISpec, &document); err != nil {
		return nil, fmt.Errorf("decode embedded OpenAPI specification: %w", err)
	}

	document["servers"] = []any{map[string]any{
		"url":         apiBasePath,
		"description": "AKS-internal trade refresher",
	}}
	document["security"] = []any{map[string]any{"BearerAuth": []any{}}}

	info, ok := document["info"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("OpenAPI specification has no info object")
	}
	info["title"] = "Aladdin Trade Refresher API"
	info["description"] = "AKS-internal proxy for every BlackRock Aladdin Trade API operation. Every operation requires `Authorization: Bearer <trade-refresher-api-key>` and a caller-supplied `X-Correlation-ID` (UUID/GUID, team name, or email). The correlation ID is returned to the caller and recorded in Dynatrace; API keys and trade payloads are never logged."

	components, ok := document["components"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("OpenAPI specification has no components object")
	}
	securitySchemes, _ := components["securitySchemes"].(map[string]any)
	if securitySchemes == nil {
		securitySchemes = make(map[string]any)
		components["securitySchemes"] = securitySchemes
	}
	securitySchemes["BearerAuth"] = map[string]any{
		"type":         "http",
		"scheme":       "bearer",
		"bearerFormat": "API key",
		"description":  "The single current Azure Key Vault secret `trade-refresher-api-key` for this environment. Enter only the key value; Swagger adds the Bearer prefix. The key is never written to application logs.",
	}
	schemas, _ := components["schemas"].(map[string]any)
	if schemas == nil {
		schemas = make(map[string]any)
		components["schemas"] = schemas
	}
	schemas["RefresherError"] = map[string]any{
		"type":     "object",
		"required": []any{"error"},
		"properties": map[string]any{
			"error":              map[string]any{"type": "string"},
			"correlationId":      map[string]any{"type": "string"},
			"blackRockRequestId": map[string]any{"type": "string"},
			"unknownOutcome":     map[string]any{"type": "boolean"},
		},
	}
	schemas["BlackRockError"] = map[string]any{
		"type":     "object",
		"required": []any{"code", "message"},
		"properties": map[string]any{
			"code":    map[string]any{"type": "string", "maxLength": 40},
			"message": map[string]any{"type": "string"},
		},
	}

	paths, ok := document["paths"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("OpenAPI specification has no paths object")
	}
	for _, pathValue := range paths {
		pathItem, ok := pathValue.(map[string]any)
		if !ok {
			continue
		}
		for _, method := range []string{"get", "post", "put", "patch", "delete"} {
			operation, ok := pathItem[method].(map[string]any)
			if !ok {
				continue
			}
			operation["parameters"] = proxyParameters(operation["parameters"])
			operation["description"] = proxyOperationDescription(operation["description"])
			addProxyResponses(operation)
		}
	}

	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode proxy OpenAPI specification: %w", err)
	}
	return encoded, nil
}

func proxyOperationDescription(value any) string {
	description, _ := value.(string)
	return description + "\n\n**Required refresher headers:** `Authorization: Bearer <api-key>` and `X-Correlation-ID`. In Swagger, select **Authorize**, enter the key without `Bearer`, and provide a traceable correlation ID on the operation."
}

func addProxyResponses(operation map[string]any) {
	responses, ok := operation["responses"].(map[string]any)
	if !ok {
		return
	}
	if success, ok := responses["200"].(map[string]any); ok {
		addResponseHeader(success, "X-Correlation-ID", map[string]any{
			"description": "Caller-supplied correlation identifier.",
			"schema":      map[string]any{"type": "string"},
		})
	}
	responses["400"] = refresherOrUpstreamErrorResponse("The refresher or BlackRock rejected the request.")
	responses["401"] = refresherOrUpstreamErrorResponse("The refresher API key or BlackRock credentials were invalid.")
	responses["502"] = refresherErrorResponse("The BlackRock request failed or returned an unreadable response.", true)
	responses["503"] = refresherErrorResponse("Refresher API-key authentication or the trade proxy is unavailable.", false)
}

func refresherOrUpstreamErrorResponse(description string) map[string]any {
	response := map[string]any{
		"description": description,
		"content": map[string]any{"application/json": map[string]any{
			"schema": map[string]any{"oneOf": []any{
				map[string]any{"$ref": "#/components/schemas/RefresherError"},
				map[string]any{"$ref": "#/components/schemas/BlackRockError"},
			}},
		}},
	}
	addCorrelationResponseHeader(response)
	return response
}

func refresherErrorResponse(description string, unknownOutcome bool) map[string]any {
	response := map[string]any{
		"description": description,
		"content": map[string]any{"application/json": map[string]any{
			"schema": map[string]any{"$ref": "#/components/schemas/RefresherError"},
		}},
	}
	addCorrelationResponseHeader(response)
	if unknownOutcome {
		addResponseHeader(response, "X-Trade-Unknown-Outcome", map[string]any{
			"description": "True when a write may have reached BlackRock and must be reconciled before retrying.",
			"schema":      map[string]any{"type": "boolean"},
		})
	}
	return response
}

func addCorrelationResponseHeader(response map[string]any) {
	addResponseHeader(response, "X-Correlation-ID", map[string]any{
		"description": "Caller-supplied correlation identifier.",
		"schema":      map[string]any{"type": "string"},
	})
}

func addResponseHeader(response map[string]any, name string, header map[string]any) {
	headers, _ := response["headers"].(map[string]any)
	if headers == nil {
		headers = make(map[string]any)
		response["headers"] = headers
	}
	headers[name] = header
}

func proxyParameters(value any) []any {
	parameters, _ := value.([]any)
	result := make([]any, 0, len(parameters)+1)
	for _, value := range parameters {
		parameter, ok := value.(map[string]any)
		if !ok {
			result = append(result, value)
			continue
		}
		name, _ := parameter["name"].(string)
		if name == "VND.com.blackrock.Request-ID" || name == "VND.com.blackrock.Origin-Timestamp" {
			continue
		}
		result = append(result, value)
	}
	return append(result, map[string]any{
		"name":        "X-Correlation-ID",
		"in":          "header",
		"required":    true,
		"description": "Caller-supplied trace identifier: UUID/GUID, team name, or email. It is returned in the response and written to logs.",
		"schema": map[string]any{
			"type":      "string",
			"minLength": 1,
			"maxLength": correlationIDMaxLength,
		},
		"example": "investment-operations",
	})
}
