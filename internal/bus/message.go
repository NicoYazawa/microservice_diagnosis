package bus

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Standard mfdh message header keys carried on the Kafka envelope. Headers keep
// routing metadata (source, session, contract version) visible without forcing
// consumers to decode the payload.
const (
	HeaderContentType   = "content-type"
	HeaderSource        = "mfdh-source"
	HeaderSessionID     = "mfdh-session-id"
	HeaderSchemaVersion = "mfdh-schema-version"
)

// Message is the transport-level envelope delivered by the bus. It decouples
// agents / the orchestrator from the kafka-go types so the rest of the codebase
// depends on a stable shape regardless of the underlying broker client.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Time      time.Time
}

// toKafkaMessage converts a bus Message into the kafka-go wire type.
func toKafkaMessage(m Message) kafka.Message {
	headers := make([]kafka.Header, 0, len(m.Headers))
	for k, v := range m.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return kafka.Message{
		Topic:   m.Topic,
		Key:     m.Key,
		Value:   m.Value,
		Headers: headers,
		Time:    m.Time,
	}
}

// fromKafkaMessage converts a kafka-go message back into the bus envelope.
func fromKafkaMessage(m kafka.Message) Message {
	headers := make(map[string]string, len(m.Headers))
	for _, h := range m.Headers {
		headers[h.Key] = string(h.Value)
	}
	return Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Headers:   headers,
		Time:      m.Time,
	}
}
