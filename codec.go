package gorabbitmq

type (
	// Encoder returns the JSON encoding of v.
	Encoder func(v any) ([]byte, error)

	// Decoder parses JSON-encoded data and stores the result
	// in the value pointed to by v. If v is nil or not a pointer,
	// Unmarshal returns an InvalidUnmarshalError.
	Decoder func(data []byte, v any) error
)

type codec struct {
	Encoder Encoder
	Decoder Decoder
}
