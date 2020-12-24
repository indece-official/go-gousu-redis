package gousuredis

import (
	"github.com/gomodule/redigo/redis"
	"github.com/indece-official/go-gousu"
)

// MockService for simply mocking IService
type MockService struct {
	gousu.MockService

	GetPoolFunc         func() *redis.Pool
	GetFunc             func(key string) ([]byte, error)
	SetFunc             func(key string, data []byte) error
	SetNXPXFunc         func(key string, data []byte, timeoutMS int) error
	DelFunc             func(key string) error
	ExistsFunc          func(key string) (bool, error)
	ScanFunc            func(pattern string, cursor int) (int, []string, error)
	RPushFunc           func(key string, data []byte) (int, error)
	LPushFunc           func(key string, data []byte) (int, error)
	LRangeFunc          func(key string, start int, stop int) ([][]byte, error)
	LPopFunc            func(key string) ([]byte, error)
	RPopFunc            func(key string) ([]byte, error)
	BLPopFunc           func(key string, timeout int) ([]byte, error)
	HGetFunc            func(key string, field string) ([]byte, error)
	HSetFunc            func(key string, field string, data []byte) error
	HScanFunc           func(key string, cursor int) (int, map[string][]byte, error)
	HKeysFunc           func(key string) ([][]byte, error)
	HDelFunc            func(key string, field string) error
	LIndexFunc          func(key string, position int) ([]byte, error)
	LLenFunc            func(key string) (int, error)
	SubscribeFunc       func(channels []string) (chan Message, ISubscription, error)
	PublishFunc         func(channel string, data []byte) error
	GetPoolFuncCalled   int
	GetFuncCalled       int
	SetFuncCalled       int
	SetNXPXFuncCalled   int
	DelFuncCalled       int
	ExistsFuncCalled    int
	ScanFuncCalled      int
	RPushFuncCalled     int
	LPushFuncCalled     int
	LRangeFuncCalled    int
	LPopFuncCalled      int
	RPopFuncCalled      int
	BLPopFuncCalled     int
	HGetFuncCalled      int
	HSetFuncCalled      int
	HScanFuncCalled     int
	HKeysFuncCalled     int
	HDelFuncCalled      int
	LIndexFuncCalled    int
	LLenFuncCalled      int
	SubscribeFuncCalled int
	PublishFuncCalled   int
}

// MockService implements IService
var _ (IService) = (*MockService)(nil)

// GetPool calls GetPoolFunc and increases GetPoolFuncCalled
func (s *MockService) GetPool() *redis.Pool {
	s.GetPoolFuncCalled++

	return s.GetPoolFunc()
}

// Get calls GetFunc and increases GetFuncCalled
func (s *MockService) Get(key string) ([]byte, error) {
	s.GetFuncCalled++

	return s.GetFunc(key)
}

// Set calls SetFunc and increases SetFuncCalled
func (s *MockService) Set(key string, data []byte) error {
	s.SetFuncCalled++

	return s.SetFunc(key, data)
}

// SetNXPX calls SetNXPXFunc and increases SetNXPXFuncCalled
func (s *MockService) SetNXPX(key string, data []byte, timeoutMS int) error {
	s.SetNXPXFuncCalled++

	return s.SetNXPXFunc(key, data, timeoutMS)
}

// Del calls DelFunc and increases DelFuncCalled
func (s *MockService) Del(key string) error {
	s.DelFuncCalled++

	return s.DelFunc(key)
}

// Exists calls ExistsFunc and increases ExistsFuncCalled
func (s *MockService) Exists(key string) (bool, error) {
	s.ExistsFuncCalled++

	return s.ExistsFunc(key)
}

// Scan calls ScanFunc and increases ScanFuncCalled
func (s *MockService) Scan(pattern string, cursor int) (int, []string, error) {
	s.ScanFuncCalled++

	return s.ScanFunc(pattern, cursor)
}

// RPush calls RPushFunc and increases RPushFuncCalled
func (s *MockService) RPush(key string, data []byte) (int, error) {
	s.RPushFuncCalled++

	return s.RPushFunc(key, data)
}

// LPush calls LPushFunc and increases LPushFuncCalled
func (s *MockService) LPush(key string, data []byte) (int, error) {
	s.LPushFuncCalled++

	return s.LPushFunc(key, data)
}

// LRange calls LRangeFunc and increases LRangeFuncCalled
func (s *MockService) LRange(key string, start int, stop int) ([][]byte, error) {
	s.LRangeFuncCalled++

	return s.LRangeFunc(key, start, stop)
}

// LPop calls LPopFunc and increases LPopFuncCalled
func (s *MockService) LPop(key string) ([]byte, error) {
	s.LPopFuncCalled++

	return s.LPopFunc(key)
}

// RPop calls RPopFunc and increases RPopFuncCalled
func (s *MockService) RPop(key string) ([]byte, error) {
	s.RPopFuncCalled++

	return s.RPopFunc(key)
}

// BLPop calls BLPopFunc and increases BLPopFuncCalled
func (s *MockService) BLPop(key string, timeout int) ([]byte, error) {
	s.BLPopFuncCalled++

	return s.BLPopFunc(key, timeout)
}

// HGet calls GetFunc and increases GetFuncCalled
func (s *MockService) HGet(key string, field string) ([]byte, error) {
	s.HGetFuncCalled++

	return s.HGetFunc(key, field)
}

// HSet calls SetFunc and increases SetFuncCalled
func (s *MockService) HSet(key string, field string, data []byte) error {
	s.HSetFuncCalled++

	return s.HSetFunc(key, field, data)
}

// HScan calls HScanFunc and increases HScanFuncCalled
func (s *MockService) HScan(key string, cursor int) (int, map[string][]byte, error) {
	s.HScanFuncCalled++

	return s.HScanFunc(key, cursor)
}

// HKeys calls HKeysFunc and increases HKeysFuncCalled
func (s *MockService) HKeys(key string) ([][]byte, error) {
	s.HKeysFuncCalled++

	return s.HKeysFunc(key)
}

// HDel calls HDelFunc and increases HDelFuncCalled
func (s *MockService) HDel(key string, field string) error {
	s.HDelFuncCalled++

	return s.HDelFunc(key, field)
}

// LIndex calls LIndexFunc and increases LIndexFuncCalled
func (s *MockService) LIndex(key string, position int) ([]byte, error) {
	s.LIndexFuncCalled++

	return s.LIndexFunc(key, position)
}

// LLen calls LLenFunc and increases LLenFuncCalled
func (s *MockService) LLen(key string) (int, error) {
	s.LLenFuncCalled++

	return s.LLenFunc(key)
}

// Subscribe calls SubscribeFunc and increases SubscribeFunc
func (s *MockService) Subscribe(channels []string) (chan Message, ISubscription, error) {
	s.SubscribeFuncCalled++

	return s.SubscribeFunc(channels)
}

// Publish calls PublishFunc and increases PublishFuncCalled
func (s *MockService) Publish(channel string, data []byte) error {
	s.PublishFuncCalled++

	return s.PublishFunc(channel, data)
}

// NewMockService creates a new initialized instance of MockService
func NewMockService() *MockService {
	return &MockService{
		MockService: gousu.MockService{
			NameFunc: func() string {
				return ServiceName
			},
		},

		GetPoolFunc: func() *redis.Pool {
			return nil
		},
		GetFunc: func(key string) ([]byte, error) {
			return []byte{}, nil
		},
		SetFunc: func(key string, data []byte) error {
			return nil
		},
		SetNXPXFunc: func(key string, data []byte, timeoutMS int) error {
			return nil
		},
		DelFunc: func(key string) error {
			return nil
		},
		ExistsFunc: func(key string) (bool, error) {
			return false, nil
		},
		ScanFunc: func(pattern string, cursor int) (int, []string, error) {
			return 0, []string{}, nil
		},
		RPushFunc: func(key string, data []byte) (int, error) {
			return 0, nil
		},
		LPushFunc: func(key string, data []byte) (int, error) {
			return 0, nil
		},
		LRangeFunc: func(key string, start int, stop int) ([][]byte, error) {
			return [][]byte{}, nil
		},
		LPopFunc: func(key string) ([]byte, error) {
			return []byte{}, nil
		},
		RPopFunc: func(key string) ([]byte, error) {
			return []byte{}, nil
		},
		BLPopFunc: func(key string, timeout int) ([]byte, error) {
			return []byte{}, nil
		},
		HGetFunc: func(key string, field string) ([]byte, error) {
			return []byte{}, nil
		},
		HSetFunc: func(key string, field string, data []byte) error {
			return nil
		},
		HScanFunc: func(key string, cursor int) (int, map[string][]byte, error) {
			return 0, map[string][]byte{}, nil
		},
		HKeysFunc: func(key string) ([][]byte, error) {
			return [][]byte{}, nil
		},
		HDelFunc: func(key string, field string) error {
			return nil
		},
		LIndexFunc: func(key string, position int) ([]byte, error) {
			return []byte{}, nil
		},
		LLenFunc: func(key string) (int, error) {
			return 0, nil
		},
		SubscribeFunc: func(channels []string) (chan Message, ISubscription, error) {
			return nil, nil, nil
		},
		PublishFunc: func(channel string, data []byte) error {
			return nil
		},
	}
}
