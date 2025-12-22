package cache

import (
	"sync"
	"time"

	"fayan/internal/models"
)

// Cache provides a simple in-memory cache with TTL
type Cache struct {
	userCache       map[string]*userCacheEntry
	userCacheMutex  sync.RWMutex
	totalUsersCache *totalUsersCacheEntry
	totalUsersMutex sync.RWMutex
	userCacheTTL    time.Duration
	totalUsersTTL   time.Duration
}

type userCacheEntry struct {
	user      *models.UserInfo
	timestamp time.Time
}

type totalUsersCacheEntry struct {
	count     int
	timestamp time.Time
}

// New creates a new Cache instance
func New(userTTL, totalUsersTTL time.Duration) *Cache {
	c := &Cache{
		userCache:     make(map[string]*userCacheEntry),
		userCacheTTL:  userTTL,
		totalUsersTTL: totalUsersTTL,
	}

	go c.cleanupRoutine()

	return c
}

// GetUser retrieves user from cache or calls the loader function
func (c *Cache) GetUser(pubkey string, loader func() (interface{}, error)) (*models.UserInfo, error) {
	c.userCacheMutex.RLock()
	if entry, exists := c.userCache[pubkey]; exists {
		if time.Since(entry.timestamp) < c.userCacheTTL {
			c.userCacheMutex.RUnlock()
			return entry.user, nil
		}
	}
	c.userCacheMutex.RUnlock()

	result, err := loader()
	if err != nil {
		return nil, err
	}

	user, ok := result.(*models.UserInfo)
	if !ok || user == nil {
		return nil, nil
	}

	c.userCacheMutex.Lock()
	c.userCache[pubkey] = &userCacheEntry{
		user:      user,
		timestamp: time.Now(),
	}
	c.userCacheMutex.Unlock()

	return user, nil
}

// GetTotalUsers retrieves total users from cache or calls the loader function
func (c *Cache) GetTotalUsers(loader func() (int, error)) (int, error) {
	c.totalUsersMutex.RLock()
	if c.totalUsersCache != nil && time.Since(c.totalUsersCache.timestamp) < c.totalUsersTTL {
		count := c.totalUsersCache.count
		c.totalUsersMutex.RUnlock()
		return count, nil
	}
	c.totalUsersMutex.RUnlock()

	count, err := loader()
	if err != nil {
		return 0, err
	}

	c.totalUsersMutex.Lock()
	c.totalUsersCache = &totalUsersCacheEntry{
		count:     count,
		timestamp: time.Now(),
	}
	c.totalUsersMutex.Unlock()

	return count, nil
}

// GetUsers retrieves multiple users from cache, using loader for cache misses
func (c *Cache) GetUsers(pubkeys []string, loader func([]string) (map[string]*models.UserInfo, error)) (map[string]*models.UserInfo, error) {
	result := make(map[string]*models.UserInfo)
	var missedPubkeys []string

	// Check cache for each pubkey
	c.userCacheMutex.RLock()
	for _, pubkey := range pubkeys {
		if entry, exists := c.userCache[pubkey]; exists {
			if time.Since(entry.timestamp) < c.userCacheTTL {
				result[pubkey] = entry.user
				continue
			}
		}
		missedPubkeys = append(missedPubkeys, pubkey)
	}
	c.userCacheMutex.RUnlock()

	// Load missed pubkeys from database
	if len(missedPubkeys) > 0 {
		loadedUsers, err := loader(missedPubkeys)
		if err != nil {
			return nil, err
		}

		// Update cache with loaded users
		c.userCacheMutex.Lock()
		now := time.Now()
		for pubkey, user := range loadedUsers {
			c.userCache[pubkey] = &userCacheEntry{
				user:      user,
				timestamp: now,
			}
			result[pubkey] = user
		}
		c.userCacheMutex.Unlock()
	}

	return result, nil
}

// cleanupRoutine periodically removes expired cache entries
func (c *Cache) cleanupRoutine() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanupUserCache()
		c.cleanupTotalUsersCache()
	}
}

func (c *Cache) cleanupUserCache() {
	c.userCacheMutex.Lock()
	defer c.userCacheMutex.Unlock()

	now := time.Now()
	for pubkey, entry := range c.userCache {
		if now.Sub(entry.timestamp) > c.userCacheTTL {
			delete(c.userCache, pubkey)
		}
	}
}

func (c *Cache) cleanupTotalUsersCache() {
	c.totalUsersMutex.Lock()
	defer c.totalUsersMutex.Unlock()

	if c.totalUsersCache != nil && time.Since(c.totalUsersCache.timestamp) > c.totalUsersTTL {
		c.totalUsersCache = nil
	}
}
