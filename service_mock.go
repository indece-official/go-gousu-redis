package gousuredis

import "github.com/indece-official/go-gousu"

// MockService for simply mocking IService
type MockService struct {
	gousu.MockService

	GetFunc           func(key string) ([]byte, error)
	SetFunc           func(key string, data []byte) error
	SetNXPXFunc       func(key string, data []byte, timeoutMS int) error
	DelFunc           func(key string) error
	RPushFunc         func(key string, data []byte) error
	LPopFunc          func(key string) ([]byte, error)
	BLPopFunc         func(key string, timeout int) ([]byte, error)
	HGetFunc          func(key string, field string) ([]byte, error)
	HSetFunc          func(key string, field string, data []byte) error
	HScanFunc         func(key string, cursor int) (int, [][]byte, error)
	HKeysFunc         func(key string) ([][]byte, error)
	LIndexFunc        func(key string, position int) ([]byte, error)
	LLenFunc          func(key string) (int, error)
	GetFuncCalled     int
	SetFuncCalled     int
	SetNXPXFuncCalled int
	DelFuncCalled     int
	RPushFuncCalled   int
	LPopFuncCalled    int
	BLPopFuncCalled   int
	HGetFuncCalled    int
	HSetFuncCalled    int
	HScanFuncCalled   int
	HKeysFuncCalled   int
	LIndexFuncCalled  int
	LLenFuncCalled    int
}

// MockService implements IService
var _ (IService) = (*MockService)(nil)

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

// RPush calls RPushFunc and increases RPushFuncCalled
func (s *MockService) RPush(key string, data []byte) error {
	s.RPushFuncCalled++

	return s.RPushFunc(key, data)
}

// LPop calls LPopFunc and increases LPopFuncCalled
func (s *MockService) LPop(key string) ([]byte, error) {
	s.LPopFuncCalled++

	return s.LPopFunc(key)
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
func (s *MockService) HScan(key string, cursor int) (int, [][]byte, error) {
	s.HScanFuncCalled++

	return s.HScanFunc(key, cursor)
}

// HKeys calls HKeysFunc and increases HKeysFuncCalled
func (s *MockService) HKeys(key string) ([][]byte, error) {
	s.HKeysFuncCalled++

	return s.HKeysFunc(key)
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

// NewMockService creates a new initialized instance of MockService
func NewMockService() *MockService {
	return &MockService{
		MockService: gousu.MockService{
			NameFunc: func() string {
				return ServiceName
			},
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
		RPushFunc: func(key string, data []byte) error {
			return nil
		},
		LPopFunc: func(key string) ([]byte, error) {
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
		HScanFunc: func(key string, cursor int) (int, [][]byte, error) {
			return 0, [][]byte{}, nil
		},
		HKeysFunc: func(key string) ([][]byte, error) {
			return [][]byte{}, nil
		},
		LIndexFunc: func(key string, position int) ([]byte, error) {
			return []byte{}, nil
		},
		LLenFunc: func(key string) (int, error) {
			return 0, nil
		},
		GetFuncCalled:     0,
		SetFuncCalled:     0,
		SetNXPXFuncCalled: 0,
		DelFuncCalled:     0,
		RPushFuncCalled:   0,
		LPopFuncCalled:    0,
		BLPopFuncCalled:   0,
		HScanFuncCalled:   0,
		HKeysFuncCalled:   0,
		LIndexFuncCalled:  0,
		LLenFuncCalled:    0,
	}
}
