package gousuredis

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
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

	GetPool() *redis.Pool
	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	SetNXPX(key string, data []byte, timeoutMS int) error
	SetPX(key string, data []byte, timeoutMS int) error
	Del(key string) error
	Exists(key string) (bool, error)
	Scan(pattern string, cursor int) (int, []string, error)
	RPush(key string, data []byte) (int, error)
	LPush(key string, data []byte) (int, error)
	LRange(key string, start int, stop int) ([][]byte, error)
	LPop(key string) ([]byte, error)
	RPop(key string) ([]byte, error)
	BLPop(key string, timeout int) ([]byte, error)
	HGet(key string, field string) ([]byte, error)
	HSet(key string, field string, data []byte) error
	HScan(key string, cursor int) (int, map[string][]byte, error)
	HKeys(key string) ([][]byte, error)
	HDel(key string, field string) error
	HLen(key string) (int, error)
	LIndex(key string, position int) ([]byte, error)
	LLen(key string) (int, error)
	Subscribe(channels []string) (chan Message, ISubscription, error)
	Publish(channel string, data []byte) error
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

// SetNXPX stores a key and its value if it does not exist with expiration time in redis
func (s *Service) SetNXPX(key string, data []byte, timeoutMS int) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, data, "NX", "PX", timeoutMS)

	return err
}

// SetPX stores a key and its value with expiration time in redis
func (s *Service) SetPX(key string, data []byte, timeoutMS int) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, data, "PX", timeoutMS)

	return err
}

// Del deletes a key from redis
func (s *Service) Del(key string) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)

	return err
}

// Exists checks if a key exists in redis
func (s *Service) Exists(key string) (bool, error) {
	conn := s.pool.Get()
	defer conn.Close()

	exists, err := redis.Int(conn.Do("EXISTS", key))
	if err != nil {
		return false, err
	}

	return exists >= 1, nil
}

// Scan scans all keys for a specific pattern and returns a list of keys
func (s *Service) Scan(pattern string, cursor int) (int, []string, error) {
	keys := make([]string, 0)

	conn := s.pool.Get()
	defer conn.Close()

	resp, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", pattern))
	if err != nil {
		return 0, nil, err
	}

	_, err = redis.Scan(resp, &cursor, &keys)
	if err != nil {
		return 0, nil, err
	}

	return cursor, keys, nil
}

// RPush appends an item to a list
func (s *Service) RPush(key string, data []byte) (int, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("RPUSH", key, data))
}

// LPush prepends an item to a list
func (s *Service) LPush(key string, data []byte) (int, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("LPUSH", key, data))
}

// LRange loads elements from a list
func (s *Service) LRange(key string, start int, stop int) ([][]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.ByteSlices(conn.Do("LRANGE", key, start, stop))
}

// LPop returns the newest item from a list (non-blocking)
func (s *Service) LPop(key string) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("LPOP", key))
}

// RPop returns the oldest item from a list (non-blocking)
func (s *Service) RPop(key string) ([]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("RPOP", key))
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
		return nil, ErrNil
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
func (s *Service) HScan(key string, cursor int) (int, map[string][]byte, error) {
	arr := make([][]byte, 0)

	conn := s.pool.Get()
	defer conn.Close()

	resp, err := redis.Values(conn.Do("HSCAN", key, cursor))
	if err != nil {
		return 0, nil, err
	}

	_, err = redis.Scan(resp, &cursor, &arr)
	if err != nil {
		return 0, nil, err
	}

	keyValues := map[string][]byte{}
	for i := range arr {
		if i > 0 && i%2 == 1 {
			keyValues[string(arr[i-1])] = arr[i]
		}
	}

	return cursor, keyValues, nil
}

// HKeys gets all field names in the hash stored at key
func (s *Service) HKeys(key string) ([][]byte, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.ByteSlices(conn.Do("HKEYS", key))
}

// HDel deletes a field from a hash
func (s *Service) HDel(key string, field string) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HDEL", key, field)

	return err
}

// HLen gets the length of the map stored at key
func (s *Service) HLen(key string) (int, error) {
	conn := s.pool.Get()
	defer conn.Close()

	return redis.Int(conn.Do("HLEN", key))
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

// Message is emitted after subscribing to a channel
type Message struct {
	Error   error
	Channel string
	Data    []byte
}

// IsError returns if an error occured
func (m *Message) IsError() bool {
	return m.Error != nil
}

// ISubscription defines the interface of Subscription
type ISubscription interface {
	Subscribe(channel ...interface{}) error
	Unsubscribe(channel ...interface{}) error
	Close() error
}

// Subscription is used to track a subscription to a channel via Subscribe(...)
type Subscription struct {
	conn *redis.PubSubConn
}

var _ (ISubscription) = (*Subscription)(nil)

// Subscribe subscribes to one or multiple channels
func (s *Subscription) Subscribe(channel ...interface{}) error {
	if s.conn == nil {
		return fmt.Errorf("No connection")
	}

	return s.conn.Subscribe(channel...)
}

// Unsubscribe unsubscribes from one or multiple channels
func (s *Subscription) Unsubscribe(channel ...interface{}) error {
	if s.conn == nil {
		return fmt.Errorf("No connection")
	}

	return s.conn.Unsubscribe(channel...)
}

// Close unsubscribes from all subscriptions and closes the connection
func (s *Subscription) Close() error {
	if s.conn == nil {
		return fmt.Errorf("No connection")
	}

	err := s.conn.Unsubscribe()
	if err != nil {
		return err
	}

	err = s.conn.Close()
	if err != nil {
		return err
	}

	s.conn = nil

	return nil
}

// GetPool returns the redis connection pool
func (s *Service) GetPool() *redis.Pool {
	return s.pool
}

// Subscribe subscribes to channels and returns a subscription
func (s *Service) Subscribe(channels []string) (chan Message, ISubscription, error) {
	conn := s.pool.Get()

	psc := &redis.PubSubConn{Conn: conn}

	if err := psc.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return nil, nil, err
	}

	output := make(chan Message, 1)

	subscription := &Subscription{
		conn: psc,
	}

	// Start a goroutine to receive notifications from the server.
	go func() {
		for subscription.conn != nil {
			switch n := subscription.conn.Receive().(type) {
			case error:
				output <- Message{
					Error: n,
				}
				return
			case redis.Message:
				output <- Message{
					Channel: n.Channel,
					Data:    n.Data,
				}
			case redis.Subscription:
				switch n.Count {
				case 0:
					// Return from the goroutine when all channels got unsubscribed
					return
				}
			}
		}
	}()

	// Start loop for pinging to check if connection is still alive
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for subscription.conn != nil {
			<-ticker.C
			// Send ping to test health of connection and server. If
			// corresponding pong is not received, then receive on the
			// connection will timeout and the receive goroutine will exit.
			if err := psc.Ping(""); err != nil {
				output <- Message{
					Error: err,
				}

				subscription.Unsubscribe()
			}
		}
	}()

	return output, subscription, nil
}

// Publish emits a message on a channel
func (s *Service) Publish(channel string, data []byte) error {
	conn := s.pool.Get()
	defer conn.Close()

	_, err := conn.Do("PUBLISH", channel, data)

	return err
}

// NewService is the ServiceFactory for redis service
func NewService(ctx gousu.IContext) gousu.IService {
	return &Service{
		log: gousu.GetLogger("service.redis"),
	}
}

// Assert NewService fullfills gousu.ServiceFactory
var _ (gousu.ServiceFactory) = NewService
