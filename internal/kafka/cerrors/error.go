package cerrors

import "errors"

var (
	ErrInvalidNativeType     = errors.New("native must be map[string]interface{} for goavro (like JSON)")
	ErrMessageTooShort       = errors.New("message too short")
	ErrUnknownMagicByte      = errors.New("unknown magic byte")
	ErrInvalidAvroNativeType = errors.New("invalid avro native type")
)
