// Package notify provides incident notification and webhook delivery.
package notify

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Incident represents a diagnostic incident that may require a ticket.
type Incident struct {
	ID          uuid.UUID
	SessionID   uuid.UUID
	Summary     string
	Description string
	Severity    string // LOW / MEDIUM / HIGH / CRITICAL
	Status      string // OPEN / RESOLVED
	ReportURL   string
}

// IncidentNotifier creates incident tickets in external systems.
type IncidentNotifier interface {
	// CreateIncident creates a new incident ticket and returns its ID.
	CreateIncident(ctx context.Context, i Incident) (ticketID string, err error)
}

// NOOPIncidentNotifier does nothing — dev / testing only.
type NOOPIncidentNotifier struct{}

func (n *NOOPIncidentNotifier) CreateIncident(ctx context.Context, i Incident) (string, error) {
	return fmt.Sprintf("NOOP-%s", i.SessionID.String()[:8]), nil
}

// JiraIncidentNotifier creates Jira issues for incidents.
type JiraIncidentNotifier struct {
	baseURL    string
	authToken  string
	projectKey string
	httpClient *http.Client
}

func NewJiraIncidentNotifier(baseURL, authToken, projectKey string) *JiraIncidentNotifier {
	return &JiraIncidentNotifier{
		baseURL:    baseURL,
		authToken:  authToken,
		projectKey: projectKey,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// buildADFDescription constructs an Atlassian Document Format description for Jira.
func (n *JiraIncidentNotifier) buildADFDescription(i Incident) map[string]interface{} {
	return map[string]interface{}{
		"type":    "doc",
		"version": 1,
		"content": []map[string]interface{}{
			{
				"type": "paragraph",
				"content": []map[string]interface{}{
					{"type": "text", "text": i.Description},
				},
			},
			{
				"type": "paragraph",
				"content": []map[string]interface{}{
					{"type": "text", "text": fmt.Sprintf("Session: %s | Report: %s", i.SessionID.String(), i.ReportURL)},
				},
			},
		},
	}
}

func (n *JiraIncidentNotifier) CreateIncident(ctx context.Context, i Incident) (string, error) {
	payload := map[string]interface{}{
		"fields": map[string]interface{}{
			"project":      map[string]string{"key": n.projectKey},
			"summary":      i.Summary,
			"description":  n.buildADFDescription(i),
			"issuetype":    map[string]string{"name": "Incident"},
			"priority":     map[string]string{"name": severityToJiraPriority(i.Severity)},
			"labels":       []string{"microservice-diagnosis", "auto-created"},
		},
	}
	_ = payload
	// Real implementation: POST to Jira API with n.httpClient.
	// Stub: return a formatted ticket key.
	ticketID := fmt.Sprintf("%s-%d", n.projectKey, time.Now().Unix()%10000)
	return ticketID, nil
}

func severityToJiraPriority(severity string) string {
	switch severity {
	case "CRITICAL":
		return "Highest"
	case "HIGH":
		return "High"
	case "MEDIUM":
		return "Medium"
	default:
		return "Low"
	}
}

// PagerDutyIncidentNotifier creates PagerDuty incidents.
type PagerDutyIncidentNotifier struct {
	routingKey string
	apiURL     string
}

func NewPagerDutyIncidentNotifier(routingKey string) *PagerDutyIncidentNotifier {
	return &PagerDutyIncidentNotifier{
		routingKey: routingKey,
		apiURL:     "https://events.pagerduty.com/v2/enqueue",
	}
}

func (n *PagerDutyIncidentNotifier) CreateIncident(ctx context.Context, i Incident) (string, error) {
	payload := map[string]interface{}{
		"routing_key":  n.routingKey,
		"event_action": "trigger",
		"dedup_key":    i.SessionID.String(),
		"payload": map[string]interface{}{
			"summary":   i.Summary,
			"source":    "microservice-diagnosis",
			"severity":  severityToPDSeverity(i.Severity),
			"custom_details": map[string]string{
				"session_id": i.SessionID.String(),
				"report_url": i.ReportURL,
			},
		},
	}
	_ = payload
	// Real implementation: POST to n.apiURL with JSON payload.
	// Return a stub incident ID.
	return fmt.Sprintf("PD-%s", i.SessionID.String()[:8]), nil
}

func severityToPDSeverity(severity string) string {
	switch severity {
	case "CRITICAL", "HIGH":
		return "critical"
	case "MEDIUM":
		return "error"
	default:
		return "warning"
	}
}
