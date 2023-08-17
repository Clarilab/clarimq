package gorabbitmq

type (
	// JSONEncoder returns the JSON encoding of v.
	JSONEncoder func(v any) ([]byte, error)

	// JSONDecoder parses JSON-encoded data and stores the result
	// in the value pointed to by v. If v is nil or not a pointer,
	// Unmarshal returns an InvalidUnmarshalError.
	JSONDecoder func(data []byte, v any) error
)

type codec struct {
	Encoder JSONEncoder
	Decoder JSONDecoder
}
