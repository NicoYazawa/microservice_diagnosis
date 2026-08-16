package bus

import (
	"errors"
	"testing"
)

func TestTopicNaming(t *testing.T) {
	if got, want := Topic("custom"), "mfdh.custom"; got != want {
		t.Errorf("Topic(custom) = %q, want %q", got, want)
	}
	if got := Topic("mfdh.custom"); got != "mfdh.custom" {
		t.Errorf("Topic(prefixed) = %q, want idempotent", got)
	}
	if TopicObservations != "mfdh.observations" || TopicCommands != "mfdh.commands" ||
		TopicEvents != "mfdh.events" || TopicDLQ != "mfdh.dlq" {
		t.Error("core topic constants diverged from the mfdh.* convention")
	}
}

func TestGroupID(t *testing.T) {
	if got, want := GroupID("agent-log", "mfdh.observations"), "mfdh-agent-log-observations"; got != want {
		t.Errorf("GroupID = %q, want %q", got, want)
	}
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	cfg := (Config{}).withDefaults()
	cfg.Brokers = []string{"b1:9092"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaulted config should validate: %v", err)
	}
	if cfg.RequiredAcks != "all" || cfg.BatchSize != 100 || cfg.ReplayIdle <= 0 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
}

func TestConfigValidationRejects(t *testing.T) {
	good := Config{Brokers: []string{"localhost:29092"}}.withDefaults()

	cases := []struct {
		name string
		mut  func(*Config)
		want error
	}{
		{"no brokers", func(c *Config) { c.Brokers = nil }, ErrEmptyBrokers},
		{"empty broker addr", func(c *Config) { c.Brokers = []string{" "} }, ErrEmptyBroker},
		{"bad acks", func(c *Config) { c.RequiredAcks = "sometimes" }, ErrInvalidRequiredAcks},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := good
			tc.mut(&c)
			c = c.withDefaults()
			if err := c.Validate(); !errors.Is(err, tc.want) {
				t.Errorf("Validate() = %v, want %v", err, tc.want)
			}
		})
	}

	cc := ConsumerConfig{Topic: "", GroupID: "g"}
	if !errors.Is(cc.Validate(), ErrEmptyTopic) {
		t.Errorf("empty topic should be rejected, got %v", cc.Validate())
	}
	cc = ConsumerConfig{Topic: "t", GroupID: ""}
	if !errors.Is(cc.Validate(), ErrEmptyGroupID) {
		t.Errorf("empty group id should be rejected, got %v", cc.Validate())
	}
	cc = ConsumerConfig{Topic: "t", GroupID: "g", StartOffset: 7}
	if !errors.Is(cc.Validate(), ErrInvalidStartOffset) {
		t.Errorf("bogus start offset should be rejected, got %v", cc.Validate())
	}
}
