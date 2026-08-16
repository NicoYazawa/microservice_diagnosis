package observation

import (
	"errors"
	"math"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
)

// validBase builds a valid Observation (business fields complete; system fields filled by New).
func validBase() *observationv1.Observation {
	return &observationv1.Observation{
		SessionId:     "session-01",
		Source:        "agent-log",
		Type:          observationv1.EvidenceType_EVIDENCE_TYPE_LOG,
		SubType:       "log_pattern",
		Confidence:    0.95,
		Severity:      observationv1.Severity_SEVERITY_ERROR,
		TargetService: "order-service",
		Correlations:  map[string]string{"trace_id": "t-1", "span_id": "s-1"},
		Labels:        map[string]string{"env": "prod", "region": "cn-north"},
		DetailJson:    `{"pattern":"connection pool exhausted"}`,
	}
}

func mustNew(t *testing.T, o *observationv1.Observation) *observationv1.Observation {
	t.Helper()
	got, err := New(o)
	if err != nil {
		t.Fatalf("New() should not fail: %v", err)
	}
	return got
}

// TestNewFillsSystemFields: the constructor auto-generates id / timestamp / schema_version.
func TestNewFillsSystemFields(t *testing.T) {
	o := mustNew(t, validBase())

	if o.Id == "" {
		t.Error("id should be auto-generated")
	}
	if o.SchemaVersion != CurrentSchemaVersion {
		t.Errorf("schema_version = %d, want %d", o.SchemaVersion, CurrentSchemaVersion)
	}
	if o.Timestamp == nil || o.Timestamp.AsTime().IsZero() {
		t.Error("timestamp should be auto-filled and non-zero")
	}
	if err := Validate(o); err != nil {
		t.Errorf("auto-generated observation should pass validation: %v", err)
	}
}

// TestValidateNil: nil is rejected.
func TestValidateNil(t *testing.T) {
	if err := Validate(nil); !errors.Is(err, ErrNilObservation) {
		t.Errorf("Validate(nil) = %v, want ErrNilObservation", err)
	}
}

// TestValidateRequiredFields: missing required fields are rejected.
func TestValidateRequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*observationv1.Observation)
		want error
	}{
		{"empty id", func(o *observationv1.Observation) { o.Id = "" }, ErrEmptyID},
		{"empty session_id", func(o *observationv1.Observation) { o.SessionId = "" }, ErrEmptySessionID},
		{"empty source", func(o *observationv1.Observation) { o.Source = "" }, ErrEmptySource},
		{"unspecified type", func(o *observationv1.Observation) { o.Type = observationv1.EvidenceType_EVIDENCE_TYPE_UNSPECIFIED }, ErrUnspecifiedType},
		{"unspecified severity", func(o *observationv1.Observation) { o.Severity = observationv1.Severity_SEVERITY_UNSPECIFIED }, ErrUnspecifiedSeverity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := validBase()
			o.Id = "id-x"
			o.SchemaVersion = CurrentSchemaVersion
			tc.mut(o)
			if err := Validate(o); !errors.Is(err, tc.want) {
				t.Errorf("Validate() = %v, want to contain %v", err, tc.want)
			}
		})
	}
}

// TestValidateConfidence: out-of-range confidence (including NaN / +/-Inf) is rejected.
func TestValidateConfidence(t *testing.T) {
	for _, c := range []float64{-0.1, 1.01, math.NaN(), math.Inf(1), math.Inf(-1)} {
		o := validBase()
		o.Id = "id-x"
		o.SchemaVersion = CurrentSchemaVersion
		o.Confidence = c
		if err := Validate(o); !errors.Is(err, ErrConfidenceOutOfRange) {
			t.Errorf("confidence=%v should be rejected, got err=%v", c, err)
		}
	}
}

// TestValidateSchemaVersion: schema_version compatibility range validation.
func TestValidateSchemaVersion(t *testing.T) {
	for _, v := range []int64{MinSchemaVersion - 1, 0, MaxSchemaVersion + 1, 99} {
		o := validBase()
		o.Id = "id-x"
		o.SchemaVersion = v
		if err := Validate(o); !errors.Is(err, ErrUnsupportedSchemaVersion) {
			t.Errorf("schema_version=%d should be rejected, got err=%v", v, err)
		}
	}
	if err := Validate(validBaseWithSchema(CurrentSchemaVersion)); err != nil {
		t.Errorf("schema_version=%d should pass: %v", CurrentSchemaVersion, err)
	}
}

// TestProtoRoundTrip: protobuf binary round-trip.
func TestProtoRoundTrip(t *testing.T) {
	o := mustNew(t, validBase())

	data, err := proto.Marshal(o)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	var got observationv1.Observation
	if err := proto.Unmarshal(data, &got); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if !proto.Equal(o, &got) {
		t.Error("protobuf round-trip changed the observation")
	}
}

// TestJSONRoundTrip: protojson round-trip (marshal -> unmarshal -> deep compare).
func TestJSONRoundTrip(t *testing.T) {
	o := mustNew(t, validBase())

	data, err := ToJSON(o)
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	got, err := FromJSON(data)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if !Equal(o, got) {
		t.Errorf("JSON round-trip changed the observation\noriginal: %s\nrestored: %s", data, mustJSON(t, got))
	}
}

// TestFromJSONRejectsUnknownField: unknown fields are rejected (protojson strict mode).
func TestFromJSONRejectsUnknownField(t *testing.T) {
	bad := `{"id":"x","session_id":"s","source":"agent-log",` +
		`"type":"EVIDENCE_TYPE_LOG","severity":"SEVERITY_ERROR",` +
		`"confidence":0.5,"schema_version":1,"unknown_field":1}`
	if _, err := FromJSON([]byte(bad)); err == nil {
		t.Fatal("JSON with an unknown field should be rejected")
	}
}

// TestFromJSONRejectsIllegalEnum: illegal enum values are rejected.
func TestFromJSONRejectsIllegalEnum(t *testing.T) {
	bad := `{"id":"x","session_id":"s","source":"agent-log",` +
		`"type":"EVIDENCE_TYPE_99","severity":"SEVERITY_ERROR",` +
		`"confidence":0.5,"schema_version":1}`
	if _, err := FromJSON([]byte(bad)); err == nil {
		t.Fatal("illegal enum value should be rejected")
	}
	if !strings.Contains(errText(t, bad), "invalid value") && !strings.Contains(errText(t, bad), "unknown") {
		t.Errorf("unexpected illegal enum error message: %s", errText(t, bad))
	}
}

// TestFromJSONValidatesSchemaVersion: schema_version validation runs after deserialization.
func TestFromJSONValidatesSchemaVersion(t *testing.T) {
	bad := `{"id":"x","session_id":"s","source":"agent-log",` +
		`"type":"EVIDENCE_TYPE_LOG","severity":"SEVERITY_ERROR",` +
		`"confidence":0.5,"schema_version":42}`
	if _, err := FromJSON([]byte(bad)); !errors.Is(err, ErrUnsupportedSchemaVersion) {
		t.Errorf("schema_version=42 should be rejected, got err=%v", err)
	}
}

func mustJSON(t *testing.T, o *observationv1.Observation) []byte {
	t.Helper()
	b, err := ToJSON(o)
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	return b
}

func errText(t *testing.T, data string) string {
	t.Helper()
	_, err := FromJSON([]byte(data))
	if err == nil {
		t.Fatal("expected parsing to fail")
	}
	return err.Error()
}

func validBaseWithSchema(v int64) *observationv1.Observation {
	o := validBase()
	o.Id = "id-x"
	o.SchemaVersion = v
	return o
}
