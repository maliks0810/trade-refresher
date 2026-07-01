package types

import (
	"context"
	"errors"
	"time"

	"github.com/patrickmn/go-cache"
)

// Global representation of the TTLCache interface
var InMemoryCache TTLCache

// ErrCacheNotInstantiated is returned when the internal cache object has not been instantiated
var ErrCacheNotInstantiated = errors.New("cache has not been instantiated properly")
// ErrInvalidCacheKey is returned when the provided key is empty string
var ErrInvalidCacheKey = errors.New("invalid cache key provided")
// ErrCannotParseCacheValue is returned if the value stored in the cache cannot be cast from interface{} to string
var ErrCannotParseCacheValue = errors.New("unable to parse cache value to string")

// NewInMemoryCache executes an instantiation of a private structure that implements
// the TTLCache interface.  The expiration time allows for customization of how long to
// hold the value within the in-memory cache.
func NewInMemoryCache(expiration time.Duration) {
	InMemoryCache = &internalCache{
		cache: cache.New(expiration, 12 * time.Hour),
		handlers: make(map[string]func(context.Context)),
	}
}

type internalCache struct {
	cache *cache.Cache
	handlers map[string]func(context.Context)
}

type getter interface {
	// Get executes a lookup against an in-memory cache against the provided key.  If the value
	// is found in the cache, it is immediately returned.  If the value is expired, the method
	// will execute the provided expiration handler function to reload the cache value.
	Get(string) (string, error)
}

type setter interface {
	// Set executes a in-memory cache set to store a value for a predetermined expiration time.
	// The expiration function will be stored for later execution when attempting to retrieve an
	// expired cache value.
	Set(string, string, func(context.Context)) (bool, error)
}

// TTLCache is an interface representing the ability to retrieve and insert values to an
// in-memory cache with an customizable expiration.  When retrieving values that have expired
// expiration handler functions will be executed to retrieve current values prior to re-insertion
// into the cache
type TTLCache interface {
	getter
	setter
}

// Get implements the getter Get method
func (c *internalCache) Get(key string) (string, error) {
	if c.cache == nil {
		return "", ErrCacheNotInstantiated
	}
	if key == "" {
		return "", ErrInvalidCacheKey
	}

	v, ok := c.cache.Get(key)
	if !ok {
		h, ok := c.handlers[key]
		if !ok {
			return "", errors.New("cannot renew cache, missing expiration handler")
		}
		h(context.TODO())
		v, ok := c.cache.Get(key)
		if !ok {
			return "", errors.New("unable to retrieve cache value for the provided key")
		}

		value, ok := v.(string)
		if !ok {
			return "", ErrCannotParseCacheValue
		}
		c.Set(key, value, h)
		return value, nil
	}

	value, ok := v.(string)
	if !ok {
		return "", ErrCannotParseCacheValue
	}

	return value, nil
}

// Set implements the setter Set method
func (c *internalCache) Set(key string, value string, onExpiration func(ctx context.Context)) (bool, error) {
	if c.cache == nil {
		return false, ErrCacheNotInstantiated
	}
	if key == "" {
		return false, ErrInvalidCacheKey
	}
	if onExpiration == nil {
		return false, errors.New("invalid expiration function provided")
	}

	c.handlers[key] = onExpiration
	c.cache.Set(key, value, cache.DefaultExpiration)

	return true, nil
}
