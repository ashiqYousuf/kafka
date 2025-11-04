package schema

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	sr "github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockRegistryClient mocks the IRegistryClient interface
type MockRegistryClient struct {
	mock.Mock
}

func (m *MockRegistryClient) GetSchema(schemaID int) (*sr.Schema, error) {
	args := m.Called(schemaID)
	if sch, ok := args.Get(0).(*sr.Schema); ok {
		return sch, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockRegistryClient) CreateSchema(subject string, schema string, schemaType sr.SchemaType, references ...sr.Reference) (*sr.Schema, error) {
	args := m.Called(subject, schema, schemaType)
	if sch, ok := args.Get(0).(*sr.Schema); ok {
		return sch, args.Error(1)
	}
	return nil, args.Error(1)
}

func TestGetSchemaByID(t *testing.T) {
	type test struct {
		Name       string
		Id         int
		wantSchema *sr.Schema
		wantErr    error
		setUpMocks func(client *MockRegistryClient)
	}

	tests := []test{
		{
			Name:       "GetSchemaByID_CacheMiss_Success",
			Id:         1234,
			wantSchema: &sr.Schema{},
			wantErr:    nil,
			setUpMocks: func(client *MockRegistryClient) {
				client.On("GetSchema", mock.MatchedBy(func(id int) bool {
					return id == 1234
				})).
					Return(&sr.Schema{}, nil)
			},
		},
		{
			Name:       "GetSchemaByID_CacheHit_Success",
			Id:         1234,
			wantSchema: &sr.Schema{},
			wantErr:    nil,
			setUpMocks: func(client *MockRegistryClient) {
				client.On("GetSchema", mock.MatchedBy(func(id int) bool {
					return id == 1234
				})).
					Return(&sr.Schema{}, nil)
			},
		},
	}

	mockClient := new(MockRegistryClient)
	SetSchemaRegistryClient(mockClient)
	for _, tc := range tests {
		tc.setUpMocks(mockClient)
		t.Run(tc.Name, func(t *testing.T) {
			_, err := GetSchemaRegistryClient().GetSchemaByID(tc.Id)
			if err != nil {
				assert.EqualError(t, err, tc.wantErr.Error(), "got %v want %v", err, tc.wantErr)
			}
		})
	}
}

func TestRegisterOrGetID(t *testing.T) {
	type test struct {
		Name       string
		Subject    string
		SchemaStr  string
		wantID     int
		wantErr    error
		setUpMocks func(client *MockRegistryClient)
	}

	tests := []test{
		{
			Name:      "RegisterOrGetID_Success",
			Subject:   "user-topic-value",
			wantErr:   nil,
			SchemaStr: `{"type": "record","name": "User","fields": [{"name": "id", "type": "string"}]}`,
			wantID:    0,
			setUpMocks: func(client *MockRegistryClient) {
				client.On("CreateSchema", mock.MatchedBy(func(subject string) bool {
					return subject == "user-topic-value"
				}), mock.MatchedBy(func(schemaStr string) bool {
					return strings.Contains(schemaStr, "User")
				}), mock.MatchedBy(func(stype sr.SchemaType) bool {
					return true
				})).
					Return(&sr.Schema{}, nil)
			},
		},
	}

	for _, tc := range tests {
		mockClient := new(MockRegistryClient)
		SetSchemaRegistryClient(mockClient)
		tc.setUpMocks(mockClient)
		t.Run(tc.Name, func(t *testing.T) {
			id, err := GetSchemaRegistryClient().RegisterOrGetID(tc.Subject, tc.SchemaStr, sr.SchemaType("AVRO"))
			if err != nil {
				assert.EqualError(t, err, tc.wantErr.Error(), "got %v want %v", err, tc.wantErr)
			} else {
				assert.Equal(t, id, tc.wantID, "got %v want %v", id, tc.wantID)
			}
		})
	}
}

func TestAvroSerialize_And_Deserialize(t *testing.T) {
	mockClient := new(MockRegistryClient)
	SetSchemaRegistryClient(mockClient)

	schemaStr := `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`
	sch := &sr.Schema{} // default id of 0 (couldn't set its values as it's fields are not exported)
	mockClient.On("CreateSchema", mock.Anything, mock.Anything, mock.Anything).Return(sch, nil)

	rc := GetSchemaRegistryClient()
	native := map[string]interface{}{"id": "abc123"}

	encoded, err := rc.AvroSerialize("topic-value", schemaStr, native)
	assert.NoError(t, err)
	assert.NotNil(t, encoded)
	assert.Equal(t, MagicByte, encoded[0])
	assert.Equal(t, uint32(0), binary.BigEndian.Uint32(encoded[1:1+SchemaIDSize]))

	rc.cache.Delete(0)
	mockClient.On("GetSchema", mock.Anything).Return(nil, errors.New("internal error"))
	schemaID, decoded, err := rc.AvroDeserialize(encoded)
	assert.Error(t, err)
	assert.Equal(t, schemaID, 0)
	assert.Equal(t, decoded, map[string]interface{}(nil))
	mockClient.AssertExpectations(t)
}
