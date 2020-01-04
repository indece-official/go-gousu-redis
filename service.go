package gousuredis

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/indece-official/go-gousu"
	"github.com/namsral/flag"
)

var (
	redisHost = flag.String("redis_host", "127.0.0.1", "Redis host")
	redisPort = flag.Int("redis_port", 6379, "Redis port")
)

// IRedisService defines the interface of the redis service
type IRedisService interface {
	gousu.IService

	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	SetNXPX(key string, data []byte, timeoutMS int) error
	Del(key string) error
	RPush(key string, data []byte) error
	LPop(key string) ([]byte, error)
	BLPop(key string, timeout int) ([]byte, error)
	HScan(key string, cursor int) (int, [][]byte, error)
	HKeys(key string) ([][]byte, error)
	LIndex(key string, position int) ([]byte, error)
	LLen(key string) (int, error)
}

// RedisService provides a service for basic redis client functionality
//
// Used flags:
//   * redis_host Hostname of redis service
//   * redis_port Port of redis service
type RedisService struct {
	log  *gousu.Log
	pool *redis.Pool
}

var _ IRedisService = (*RedisService)(nil)

// Start connects to the redis pool
func (s *RedisService) Start() error {
	s.log.Infof("Connecting to redis on %s:%d ...", *redisHost, *redisPort)

	s.pool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", *redisHost, *redisPort))
			if err != nil {
				return nil, err
			}
			return c, err
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("PING")
	if err != nil {
		return fmt.Errorf("Can't connect to redis: %s", err)
	}

	return nil
}

// Health checks the health of the RedisService by pinging the redis database
func (s *RedisService) Health() error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("PING")

	if err != nil {
		return fmt.Errorf("Redis service unhealthy: %s", err)
	}

	return nil
}

// Stop closes all redis pool connections
func (s *RedisService) Stop() error {
	return s.pool.Close()
}

// Get retrieves a key's value from redis
func (s *RedisService) Get(key string) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("GET", key))
}

// Set stores a key and its value in redis
func (s *RedisService) Set(key string, data []byte) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, data)

	return err
}

// SetNXPX stores a key and its value with expiration time in redis
func (s *RedisService) SetNXPX(key string, data []byte, timeoutMS int) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, data, "NX", "PX", timeoutMS)

	return err
}

// Del deletes a key from redis
func (s *RedisService) Del(key string) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)

	return err
}

// RPush appends an item to a list
func (s *RedisService) RPush(key string, data []byte) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", key, data)

	return err
}

// LPop returns the newest item from a list (non-blocking)
func (s *RedisService) LPop(key string) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("LPOP", key))
}

// BLPop waits for a new item in a list (blocking with timeout)
func (s *RedisService) BLPop(key string, timeout int) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("BLPOP", key, timeout))
}

// HScan scans a hash map and returns a list of field-value-tupples
func (s *RedisService) HScan(key string, cursor int) (int, [][]byte, error) {
	arr := make([][]byte, 0)

	conn := s.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HSCAN", key, cursor))
	if err != nil {
		return 0, nil, err
	}

	values, err = redis.Scan(values, &cursor, &arr)
	if err != nil {
		return 0, nil, err
	}

	return cursor, arr, nil
}

// HKeys gets all field names in the hash stored at key
func (s *RedisService) HKeys(key string) ([][]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.ByteSlices(conn.Do("HKEYS", key))
}

// LIndex gets the element at index in the list stored at key
func (s *RedisService) LIndex(key string, position int) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("LINDEX", key, position))
}

// LLen gets the length of the list stored at key
func (s *RedisService) LLen(key string) (int, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("LLEN", key))
}

// NewRedisService creates a new initialized instance of RedisService
func NewRedisService() *RedisService {
	return &RedisService{
		log: gousu.GetLogger("service.redis"),
	}
}
