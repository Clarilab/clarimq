package clarimq

// Publishing is an interface for messages that are published to a broker.
type Publishing interface {
	ID() string
	GetTargets() []string
	GetData() any
	GetOptions() *PublishOptions
}

type publishing struct {
	PublishingID string
	Targets      []string
	Data         any
	Options      *PublishOptions
}

// ID implements the Publishing interface.
func (p *publishing) ID() string {
	return p.PublishingID
}

// GetTargets implements the Publishing interface.
func (p *publishing) GetTargets() []string {
	return p.Targets
}

// GetData implements the Publishing interface.
func (p *publishing) GetData() any {
	return p.Data
}

// GetOptions implements the Publishing interface.
func (p *publishing) GetOptions() *PublishOptions {
	return p.Options
}
