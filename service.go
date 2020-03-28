package gousuredis

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/indece-official/go-gousu"
	"github.com/namsral/flag"
)

// ServiceName defines the name of redis service used for dependency injection
const ServiceName = "redis"

var (
	redisHost = flag.String("redis_host", "127.0.0.1", "Redis host")
	redisPort = flag.Int("redis_port", 6379, "Redis port")
)

// ErrNil is the error returned if no matching data was found
var ErrNil = redis.ErrNil

// IService defines the interface of the redis service
type IService interface {
	gousu.IService

	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	SetNXPX(key string, data []byte, timeoutMS int) error
	Del(key string) error
	RPush(key string, data []byte) error
	LPop(key string) ([]byte, error)
	BLPop(key string, timeout int) ([]byte, error)
	HGet(key string, field string) ([]byte, error)
	HSet(key string, field string, data []byte) error
	HScan(key string, cursor int) (int, [][]byte, error)
	HKeys(key string) ([][]byte, error)
	LIndex(key string, position int) ([]byte, error)
	LLen(key string) (int, error)
}

// Service provides a service for basic redis client functionality
//
// Used flags:
//   * redis_host Hostname of redis service
//   * redis_port Port of redis service
type Service struct {
	log  *gousu.Log
	pool *redis.Pool
}

var _ IService = (*Service)(nil)

// Name returns the name of redis service from ServiceName
func (s *Service) Name() string {
	return ServiceName
}

// Start connects to the redis pool
func (s *Service) Start() error {
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

// Health checks the health of the Service by pinging the redis database
func (s *Service) Health() error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("PING")

	if err != nil {
		return fmt.Errorf("Redis service unhealthy: %s", err)
	}

	return nil
}

// Stop closes all redis pool connections
func (s *Service) Stop() error {
	return s.pool.Close()
}

// Get retrieves a key's value from redis
func (s *Service) Get(key string) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("GET", key))
}

// Set stores a key and its value in redis
func (s *Service) Set(key string, data []byte) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, data)

	return err
}

// SetNXPX stores a key and its value with expiration time in redis
func (s *Service) SetNXPX(key string, data []byte, timeoutMS int) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, data, "NX", "PX", timeoutMS)

	return err
}

// Del deletes a key from redis
func (s *Service) Del(key string) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)

	return err
}

// RPush appends an item to a list
func (s *Service) RPush(key string, data []byte) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", key, data)

	return err
}

// LPop returns the newest item from a list (non-blocking)
func (s *Service) LPop(key string) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("LPOP", key))
}

// BLPop waits for a new item in a list (blocking with timeout)
func (s *Service) BLPop(key string, timeout int) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	result, err := redis.ByteSlices(conn.Do("BLPOP", key, timeout))
	if err != nil {
		return nil, err
	}

	if len(result) < 2 || result[0] == nil || result[1] == nil {
		return nil, redis.ErrNil
	}

	return result[1], err
}

// HGet retrieves a hash value from redis
func (s *Service) HGet(key string, field string) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("HGET", key, field))
}

// HSet stores a key and its value in a hash in redis
func (s *Service) HSet(key string, field string, data []byte) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HSET", key, field, data)

	return err
}

// HScan scans a hash map and returns a list of field-value-tupples
func (s *Service) HScan(key string, cursor int) (int, [][]byte, error) {
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
func (s *Service) HKeys(key string) ([][]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.ByteSlices(conn.Do("HKEYS", key))
}

// LIndex gets the element at index in the list stored at key
func (s *Service) LIndex(key string, position int) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("LINDEX", key, position))
}

// LLen gets the length of the list stored at key
func (s *Service) LLen(key string) (int, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("LLEN", key))
}

// NewService is the ServiceFactory for redis service
func NewService(ctx gousu.IContext) gousu.IService {
	return &Service{
		log: gousu.GetLogger("service.redis"),
	}
}

// Assert NewService fullfills gousu.ServiceFactory
var _ (gousu.ServiceFactory) = NewService
