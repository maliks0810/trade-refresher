package auth

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	jwtverifier "github.com/okta/okta-jwt-verifier-golang"
)

// When there is no request of the bearer token thrown ErrMissingOrMalformedBearerToken
var ErrMissingOrMalformedBearerToken = errors.New("missing or malformed Bearer Token")

type OktaConfig struct {
	// Issuer is the Okta Authentication Issuer - Specific to TCW
	Issuer 				string
	// ClaimsToValidate is a map of which specific token claims to validate for the generated JWT.
	ClaimsToValidate 	map[string]string
	// ClaimsToExtract is a map of which JWT claims to extract, and which header key value they should be inserted
	ClaimsToExtract		map[string]string
	// Next will execute the next middleware call within the middleware dependency chain
	Next 				func(*fiber.Ctx) bool
	// SuccessHandler will be executed once the Validator/ClaimsHandler methods have finished without errors
	SuccessHandler 		fiber.Handler
	// ErrorHandler will be executed if the Validator and/or ClaimsHandler methods raise an error
	ErrorHandler 		fiber.ErrorHandler
	// Validator will validate the JWT token received as part of the request context, typically found in the header under the 'Authorization' key.  The Validator method
	// will ensure the token is a valid Okta JWT matching the provided claims.  Assuming the JWT is valid, a map of all attached claims will be returned.
	Validator 			func(*fiber.Ctx, string, string, map[string]string) (bool, map[string]interface{}, error)
	// Bypass will skip the JWT validation check - this should only be used for local development and/or automated testing where JWTs are not commonly provided
	Bypass				func(*fiber.Ctx, string) (bool, error)
	// ClaimsHandler will extract class from the validated JWT, and insert them into the request context header.
	ClaimsHandler 		func(*fiber.Ctx, map[string]interface{}, map[string]string) (error)
	// Scheme indicates the authorization scheme provided in the request context header under 'Authorization'.  Defaults to 'Bearer'
	Scheme 				string
	// Environment is the unique value of the execution environment for the service
	Environment			string
}

// OktaConfigDefault is a representation of default values and implementations of the OktaConfig.  Provides simplicity, but still exposes customization based on the 
// application owner desires.
var OktaConfigDefault = OktaConfig{
	// SuccessHandler will default to calling the internal Next() method to continue the middleware dependency chain
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	SuccessHandler: func(ctx *fiber.Ctx) error {
		return ctx.Next()
	},
	// ErrorHandler will default to returning an unauthorized (401) HTTP response.  This will return immediately, and not continue with any additional
	// middleware or request handlers.
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	//
	// err - error : The details of the error encountered during the validation process
	ErrorHandler: func (ctx *fiber.Ctx, err error) error {
		if errors.Is(err, ErrMissingOrMalformedBearerToken) {
			return ctx.Status(fiber.StatusUnauthorized).SendString(err.Error())
		}
		return ctx.Status(fiber.StatusUnauthorized).SendString("Invalid or Expired Authorization Token")
	},
	// Validator will default to validating the JWT token against the supplied issuer (TWC Okta) using the supplied claims.  If successful, the Validator method
	// will return true and a map of all claims attached to the JWT.  
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	//
	// token - string : The string representation of the JWT token parsed from the Authorization header
	//
	// claims - map[string]string : A map of all the claim key value pairs to verify
	Validator: func(ctx *fiber.Ctx, token string, issuer string, claims map[string]string) (bool, map[string]interface{}, error) {
		verifier := jwtverifier.JwtVerifier{
			Issuer: issuer,
			ClaimsToValidate: claims,
		}

		jwt, err := verifier.New().VerifyAccessToken(token)
		if err != nil {
			return false, nil, err
		}

		return true, jwt.Claims, nil
	},
	// Bypass will default to evaluating the environment to determine if JWT validation is required.
	// Environments of 'local'/'test' are defaulted to skip validation and immediately return success.
	// **IMPORTANT:** When bypassing, it will not provide any extracted claims and those values must be handled within the respective request handlers
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	//
	// environment - string : The string representation of the current runtime environment
	Bypass: func(ctx *fiber.Ctx, environment string) (bool, error) {
		if environment == "" {
			return false, errors.New("invalid environment value provided")
		}
		switch (environment) {
		case "local":
			fallthrough
		case "test":
			return true, nil
		default:
			return false, nil
		}
	},
	// ClaimsHandler extract all claims via the provided map, and insert them into the request context header using the provided keys
	// **IMPORTANT:** If Bypass returns 'true', this method will not be executed
	//
	// Parameters:
	//
	// ctx - *fiber.Ctx : The request context
	//
	// claims - map[string]interface{} : The map of claims attached to the validated JWT
	//
	// target - map[string]string : The map of desired claim keys and the associated request context headers to create with the extracted claim values
	ClaimsHandler: func(ctx *fiber.Ctx, claims map[string]interface{}, target map[string]string) error {
		if claims == nil {
			return errors.New("invalid claims collection provided")
		}
		if target == nil {
			return errors.New("invalid target mapping provided")
		}

		for claimKey, headerKey := range target {
			claimLiteral := claims[claimKey]
			if claimValue, ok := claimLiteral.(string); ok {
				ctx.Request().Header.Add(headerKey, claimValue)
			}
		}

		return nil
	},
}

// NewOktaAuthMiddleware will instantiate a specific fiber.Handler method with the provided configurations.  This middleware handler can be attached
// to routes (typically private) to ensure Okta JWT validation is performed all incoming requests with the attached middleware.
//
// Parameters:
//
// config - OktaConfig (optional) : The custom configuration for Okta Authentication middleware
func NewOktaAuthMiddleware(config ...OktaConfig) fiber.Handler {
	cfg := oktaConfigDefault(config...)

	extractor := tokenFromHeader("Authorization", cfg.Scheme)

	return func(ctx *fiber.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		ok, err := cfg.Bypass(ctx, cfg.Environment)
		if err != nil {
			return cfg.ErrorHandler(ctx, err)
		}
		if ok {
			return cfg.SuccessHandler(ctx)
		}

		token, err := extractor(ctx)
		if err != nil {
			return cfg.ErrorHandler(ctx, err)
		}

		valid, claims, err := cfg.Validator(ctx, token, cfg.Issuer, cfg.ClaimsToValidate)
		if err == nil && valid {
			err := cfg.ClaimsHandler(ctx, claims, cfg.ClaimsToExtract)
			if err != nil {
				return cfg.ErrorHandler(ctx, err)
			}

			return cfg.SuccessHandler(ctx)
		}

		return cfg.ErrorHandler(ctx, err)
	}
}

func oktaConfigDefault(config ...OktaConfig) OktaConfig {
	if len(config) < 1 {
		return OktaConfigDefault
	}

	cfg := config[0]

	if cfg.SuccessHandler == nil {
		cfg.SuccessHandler = OktaConfigDefault.SuccessHandler
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = OktaConfigDefault.ErrorHandler
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "Bearer"
	}
	if cfg.Bypass == nil {
		cfg.Bypass = OktaConfigDefault.Bypass
	}
	if cfg.Validator == nil {
		cfg.Validator = OktaConfigDefault.Validator
	}
	if cfg.ClaimsHandler == nil {
		cfg.ClaimsHandler = OktaConfigDefault.ClaimsHandler
	}
	return cfg
}

func tokenFromHeader(header string, authScheme string) func(ctx *fiber.Ctx) (string, error) {
	return func(ctx *fiber.Ctx) (string, error) {
		auth := ctx.Get(header)
		l := len(authScheme)
		if len(auth) > 0 && l == 0 {
			return auth, nil
		}
		if len(auth) > l+1 && auth[:l] == authScheme {
			return auth[l+1:], nil
		}
		return "", ErrMissingOrMalformedBearerToken
	}
}