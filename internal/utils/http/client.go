package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"time"
)

type HttpError struct {
	Code 	int
	RawBody	[]byte
	Body 	string
}

func Options[TResp any](base string, path string, ctx context.Context) (*TResp, *HttpError, error) {
	encoded, err := build(base, path, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process request: %w", err)
	}

	return send[interface{}, TResp](encoded, http.MethodOptions, nil, nil, ctx)
}

func Head[TResp any](base string, path string, params map[string]string, headers map[string]string, ctx context.Context) (*TResp, *HttpError, error) {
	encoded, err := build(base, path, params)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process request: %w", err)
	}

	return send[interface{}, TResp](encoded, http.MethodHead, nil, headers, ctx)
}

func Get[TResp any](base string, path string, params map[string]string, headers map[string]string, ctx context.Context) (*TResp, *HttpError, error) {
	encoded, err := build(base, path, params)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process request: %w", err)
	}

	return send[interface{}, TResp](encoded, http.MethodGet, nil, headers, ctx)
}

func Post[TReq any, TResp any](base string, path string, request *TReq, params map[string]string, headers map[string]string, ctx context.Context) (*TResp, *HttpError, error) {
	encoded, err := build(base, path, params)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process request: %w", err)
	}

	return send[TReq, TResp](encoded, http.MethodPost, request, headers, ctx)
}

func Put[TReq any, TResp any](base string, path string, request *TReq, params map[string]string, headers map[string]string, ctx context.Context) (*TResp, *HttpError, error) {
	encoded, err := build(base, path, params)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process request: %w", err)
	}

	return send[TReq, TResp](encoded, http.MethodPut, request, headers, ctx)
}

func Patch[TReq any, TResp any](base string, path string, request *TReq, params map[string]string, headers map[string]string, ctx context.Context) (*TResp, *HttpError, error) {
	encoded, err := build(base, path, params)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process request: %w", err)
	}

	return send[TReq, TResp](encoded, http.MethodPatch, request, headers, ctx)
}

func Delete[TReq any, TResp any](base string, path string, request *TReq, params map[string]string, headers map[string]string, ctx context.Context) (*TResp, *HttpError, error) {
	encoded, err := build(base, path, params)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process request: %w", err)
	}

	return send[TReq, TResp](encoded, http.MethodDelete, request, headers, ctx)
}

func send[TReq any, TResp any](encoded string, method string, request *TReq, headers map[string]string, ctx context.Context) (*TResp, *HttpError, error) {
	var bodyReader *bytes.Reader = nil
	if request != nil {
		body, err := json.Marshal(request)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(body)
	}

	var req *http.Request
	var reqError error
	if request != nil {
		req, reqError = http.NewRequestWithContext(ctx, method, encoded, bodyReader)
	} else {
		req, reqError = http.NewRequestWithContext(ctx, method, encoded, nil)
	}
	if reqError != nil {
		return nil, nil, fmt.Errorf("unable instantiate an new http request: %w", reqError)
	}
	
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	client := http.Client{
		Timeout: time.Duration(5 * int(time.Minute)),
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to execute http rest request: %w", err)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to process response body: %w", err)
	}

	if !success(resp.StatusCode) {
		httpError := &HttpError{
			Code: resp.StatusCode,
			RawBody: respBytes,
			Body: string(respBytes),
		}
		return nil, httpError, fmt.Errorf("received an invalid status code: %d", resp.StatusCode)
	}

	var response TResp

	if isNilInterface(response) {
		return nil, nil, nil
	}

	err = json.Unmarshal(respBytes, &response)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to unmarshal the response body: %w", err)
	}

	return &response, nil, nil
}

func build(base string, path string, params map[string]string) (string, error) {
	f := base + path
	u, err := url.Parse(f)
	if err != nil {
		return "", fmt.Errorf("unable to parse url and path: %w", err)
	}

	if len(params) != 0 {
		p := url.Values{}
		for k, v := range params {
			p.Add(k, v)
		}
		u.RawQuery = p.Encode()
	}

	return u.String(), nil
}

func success(code int) bool {
	switch code {
	case http.StatusOK:
		fallthrough
	case http.StatusCreated:
		fallthrough
	case http.StatusAccepted:
		fallthrough
	case http.StatusNonAuthoritativeInfo:
		fallthrough
	case http.StatusNoContent:
		fallthrough
	case http.StatusResetContent:
		fallthrough
	case http.StatusPartialContent:
		fallthrough
	case http.StatusMultiStatus:
		fallthrough
	case http.StatusAlreadyReported:
		fallthrough
	case http.StatusIMUsed:
		return true
	default:
		return false
	}
}

func isNilInterface(i interface{}) bool {
	if i == nil {
      return true
   }
   switch reflect.TypeOf(i).Kind() {
   case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan:
      return reflect.ValueOf(i).IsNil()
   }
   return false
}