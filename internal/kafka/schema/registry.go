package schema

import (
	"encoding/binary"
	"sync"

	"github.com/ashiqYousuf/kafka/internal/kafka/cerrors"
	"github.com/linkedin/goavro/v2"
	sr "github.com/riferrei/srclient"
)

/*
	⚙️ Workflow – Producer → Schema Registry → Kafka → Consumer
	1 Producer serializes message using schema → sends to Schema Registry.
	2 Registry checks if schema already exists:
	3 If new, registers and returns new schema ID.
	4 If existing, returns existing ID.
	5 Producer sends message to Kafka topic → includes magic byte + schema ID + serialized data.
	6 Consumer gets message → fetches schema (by ID) from Schema Registry.
	7 Consumer deserializes message using fetched schema → reads actual data.
*/

/*
	When you send messages using SR, Kafka wants data in fixed format:
	a. 1 byte -> Magic byte always 0 - indicates SR format
	b. 4 bytes SchemaID (big endian int) [cache them locally]
	c. Remaining bytes Avro endcoded payload
*/

const (
	magicByte    byte = 0 // byte is an alias for uint8
	schemaIDSize      = 4
)

type RegistryClient struct {
	// client: schema regisrty client talks to the SchemaRegistry API.
	client *sr.SchemaRegistryClient
	// cache: to store already fetched schemas (so we don’t make HTTP calls every time)
	// whenever we get a schema by ID from the registry, we store it here for reuse
	cache sync.Map
}

// NewRegistryClient initializes a new SR client using
// Schema Registry url: http://localhost:8081
// username and password for Authentication
func NewRegistryClient(url, username, password string) *RegistryClient {
	client := sr.NewSchemaRegistryClient(url)
	if username != "" && password != "" {
		client.SetCredentials(username, password)
	}
	return &RegistryClient{
		client: client,
	}
}

// GetSchemaByID: When a consumer reads a message, it contains a schema ID.
// This function fetches the schema definition for that ID.
// Schema refers to the structure of your message stored in SR.
func (r *RegistryClient) GetSchemaByID(id int) (*sr.Schema, error) {
	if s, ok := r.cache.Load(id); ok {
		return s.(*sr.Schema), nil
	}

	schema, err := r.client.GetSchema(id)
	if err != nil {
		return nil, err
	}
	r.cache.Store(id, schema)
	return schema, nil
}

// RegisterOrGetID: When producer wants to send a new message type, it first registers the schema in Schema Registry.
// If schema already exists (same structure), Registry just returns the same ID.
// Example:   CreateSchema(subject, schemaStr, "AVRO").
// subject:   usually topic name + "-value" like: users-topic-value.
// schemaStr: JSON string representing your Avro schema. Example:
//
//	{
//	  "type": "record",
//	  "name": "User",
//	  "fields": [
//	    {"name": "id", "type": "string"},
//	    {"name": "age", "type": "int"}
//	  ]
//	}
func (r *RegistryClient) RegisterOrGetID(subject string, schemaStr string, schemaType sr.SchemaType) (int, error) {
	schema, err := r.client.CreateSchema(subject, schemaStr, sr.SchemaType(schemaType.String()))
	if err != nil {
		return 0, nil
	}
	r.cache.Store(schema.ID, schema)
	return schema.ID(), nil
}

// AvroSerialize: Convert Go data into Kafka-ready binary message that includes:
// a. Magic byte (0)
// b. Schema ID (4 bytes)
// c. Avro encoded payload
func (r *RegistryClient) AvroSerialize(subject string, schemaStr string, native interface{}) ([]byte, error) {
	id, err := r.RegisterOrGetID(subject, schemaStr, sr.Avro)
	if err != nil {
		return nil, err
	}

	// Create Avro codec from schema
	// A codec is simply an encoder/decoder
	// that can turn your Go map into Avro binary format and vice versa
	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, err
	}

	nativeMap, ok := native.(map[string]interface{})
	if !ok {
		return nil, cerrors.ErrInvalidNativeType
	}

	// Encode Go map → Avro binary
	// BinaryFromNative takes an existing []byte buffer (or nil) and returns
	// a new []byte slice with Avro-encoded data appended
	binaryData, err := codec.BinaryFromNative(nil, nativeMap)
	if err != nil {
		return nil, err
	}

	// Build final message:
	// [ magicByte (1 byte) ][ schemaID (4 bytes) ][ Avro binary data ]
	out := make([]byte, 1+schemaIDSize+len(binaryData))
	out[0] = magicByte
	binary.BigEndian.PutUint32(out[1:1+schemaIDSize], uint32(id))
	copy(out[1+schemaIDSize:], binaryData)
	return out, nil
}

// AvroDeserialize: When consumer receives Kafka message bytes,
// this function extracts schema ID and decodes data.
func (r *RegistryClient) AvroDeserialize(msg []byte) (schemaID int, native map[string]interface{}, err error) {
	if len(msg) < 1+schemaIDSize {
		return 0, nil, cerrors.ErrMessageTooShort
	}

	if msg[0] != magicByte {
		return 0, nil, cerrors.ErrUnknownMagicByte
	}

	id := int(binary.BigEndian.Uint32(msg[1 : 1+schemaIDSize]))
	schema, err := r.GetSchemaByID(id)
	if err != nil {
		return id, nil, err
	}

	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		return id, nil, err
	}

	// Decode Avro binary → Go map.
	nativeVal, _, err := codec.NativeFromBinary(msg[1+schemaIDSize:])
	if err != nil {
		return id, nil, err
	}

	if nm, ok := nativeVal.(map[string]interface{}); ok {
		return id, nm, nil
	}

	return id, nil, cerrors.ErrInvalidAvroNativeType
}
