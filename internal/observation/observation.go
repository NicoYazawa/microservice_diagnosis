// Package observation provides construction, validation, and serialization helpers
// for the unified evidence model Observation.
//
// The contract is defined in api/proto/v1/observation.proto (PLAN 4.1/4.2).
// This package is the runtime companion of the M1 contract layer: all agents publish
// and consume evidence through this model, and it owns the PLAN 11.1 contract-layer
// acceptance (round-trip, schema_version validation, illegal field rejection).
package observation

import (
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
)

// Contract versions (aligned with the observation.proto schema_version evolution rules).
const (
	// CurrentSchemaVersion is the currently supported contract version.
	CurrentSchemaVersion int64 = 1
	// MinSchemaVersion is the minimum backward-compatible contract version.
	MinSchemaVersion int64 = 1
	// MaxSchemaVersion is the maximum backward-compatible contract version.
	MaxSchemaVersion int64 = 1
)

// Validation errors (contract-layer illegal field rejection).
var (
	ErrNilObservation           = errors.New("observation: nil observation")
	ErrEmptyID                  = errors.New("observation: id is required")
	ErrEmptySessionID           = errors.New("observation: session_id is required")
	ErrEmptySource              = errors.New("observation: source is required")
	ErrUnspecifiedType          = errors.New("observation: type must be specified")
	ErrUnspecifiedSeverity      = errors.New("observation: severity must be specified")
	ErrConfidenceOutOfRange     = errors.New("observation: confidence must be in [0.0, 1.0]")
	ErrUnsupportedSchemaVersion = errors.New("observation: schema_version out of supported range")
)

// New fills in system fields based on the caller-provided business fields and validates.
// It generates the id (UUID), timestamp, and schema_version; the caller must fill in
// session_id / source / type / severity and other business fields.
func New(o *observationv1.Observation) (*observationv1.Observation, error) {
	if o == nil {
		return nil, ErrNilObservation
	}
	if o.Id == "" {
		o.Id = uuid.NewString()
	}
	if o.Timestamp == nil {
		o.Timestamp = timestamppb.Now()
	}
	o.SchemaVersion = CurrentSchemaVersion
	if err := Validate(o); err != nil {
		return nil, err
	}
	return o, nil
}

// Validate checks contract fields: required fields, enums not UNSPECIFIED,
// confidence range, and schema_version compatibility range.
func Validate(o *observationv1.Observation) error {
	if o == nil {
		return ErrNilObservation
	}
	var errs []error
	if o.Id == "" {
		errs = append(errs, ErrEmptyID)
	}
	if o.SessionId == "" {
		errs = append(errs, ErrEmptySessionID)
	}
	if o.Source == "" {
		errs = append(errs, ErrEmptySource)
	}
	if o.Type == observationv1.EvidenceType_EVIDENCE_TYPE_UNSPECIFIED {
		errs = append(errs, ErrUnspecifiedType)
	}
	if o.Severity == observationv1.Severity_SEVERITY_UNSPECIFIED {
		errs = append(errs, ErrUnspecifiedSeverity)
	}
	if o.Confidence < 0 || o.Confidence > 1 || math.IsNaN(o.Confidence) {
		errs = append(errs, ErrConfidenceOutOfRange)
	}
	if o.SchemaVersion < MinSchemaVersion || o.SchemaVersion > MaxSchemaVersion {
		errs = append(errs, fmt.Errorf("%w: got %d, supported [%d, %d]",
			ErrUnsupportedSchemaVersion, o.SchemaVersion, MinSchemaVersion, MaxSchemaVersion))
	}
	return errors.Join(errs...)
}

// Equal reports whether two observations are semantically equal (deep proto comparison).
func Equal(a, b *observationv1.Observation) bool {
	return proto.Equal(a, b)
}

// ToJSON serializes to JSON (standard protojson naming, aligned with gRPC-Gateway / OpenAPI).
func ToJSON(o *observationv1.Observation) ([]byte, error) {
	if o == nil {
		return nil, ErrNilObservation
	}
	return protojson.Marshal(o)
}

// FromJSON deserializes from JSON and validates. Strict mode: rejects unknown
// fields and illegal enum values.
func FromJSON(data []byte) (*observationv1.Observation, error) {
	var o observationv1.Observation
	if err := (protojson.UnmarshalOptions{}).Unmarshal(data, &o); err != nil {
		return nil, fmt.Errorf("observation: unmarshal json: %w", err)
	}
	if err := Validate(&o); err != nil {
		return nil, err
	}
	return &o, nil
}
