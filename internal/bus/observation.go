package bus

import (
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// This file binds the bus transport to the M1 standardized Observation contract
// (PLAN 4): agents publish evidence and the orchestrator consumes it through
// these helpers, so the payload codec lives in exactly one place.

// ObservationMessage wraps an Observation as a transport Message using the
// protobuf binary codec plus the standard mfdh headers (source / session /
// schema_version). The observation must already satisfy observation.Validate.
func ObservationMessage(o *observationv1.Observation) (Message, error) {
	if err := observation.Validate(o); err != nil {
		return Message{}, err
	}
	payload, err := proto.Marshal(o)
	if err != nil {
		return Message{}, fmt.Errorf("bus: marshal observation: %w", err)
	}
	t := time.Time{}
	if o.Timestamp != nil {
		t = o.Timestamp.AsTime()
	}
	return Message{
		Key:   []byte(o.SessionId),
		Value: payload,
		Headers: map[string]string{
			HeaderContentType:   "application/x-protobuf",
			HeaderSource:        o.Source,
			HeaderSessionID:     o.SessionId,
			HeaderSchemaVersion: strconv.FormatInt(o.SchemaVersion, 10),
		},
		Time: t,
	}, nil
}

// DecodeObservation extracts and validates an Observation from a bus message
// payload (proto binary produced by ObservationMessage).
func DecodeObservation(m Message) (*observationv1.Observation, error) {
	var o observationv1.Observation
	if err := proto.Unmarshal(m.Value, &o); err != nil {
		return nil, fmt.Errorf("bus: unmarshal observation: %w", err)
	}
	if err := observation.Validate(&o); err != nil {
		return nil, err
	}
	return &o, nil
}
