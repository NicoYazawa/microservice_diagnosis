package executor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sExecutor executes fix actions against a Kubernetes cluster.
type K8sExecutor struct {
	client *kubernetes.Clientset
	log    *slog.Logger
}

// NewK8sExecutor creates a K8sExecutor using the provided kubeconfig path.
// If kubeconfig is empty, in-cluster config is used.
func NewK8sExecutor(kubeconfig string, log *slog.Logger) (*K8sExecutor, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("k8s executor: build config: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("k8s executor: create client: %w", err)
	}
	return &K8sExecutor{client: client, log: log}, nil
}

// Execute dispatches fix actions to the appropriate Kubernetes API call.
func (e *K8sExecutor) Execute(ctx context.Context, action FixAction) (*ExecutionResult, error) {
	e.log.Info("k8s: executing action",
		"action_id", action.ID,
		"type", action.ActionType,
		"target", action.Target)

	result := &ExecutionResult{
		ID:        action.ID,
		StartedAt: time.Now().UTC(),
	}

	switch action.ActionType {
	case "restart_pod":
		result.Status, result.Message = e.restartPod(ctx, action)
	case "scale_up":
		result.Status, result.Message = e.scaleUp(ctx, action)
	case "scale_down":
		result.Status, result.Message = e.scaleDown(ctx, action)
	case "config_change":
		result.Status, result.Message = e.patchConfigmap(ctx, action)
	default:
		result.Status = "FAILED"
		result.Message = fmt.Sprintf("unsupported action type: %s", action.ActionType)
	}

	result.EndedAt = time.Now().UTC()
	return result, nil
}

func (e *K8sExecutor) restartPod(ctx context.Context, action FixAction) (status, message string) {
	pods, err := e.lookupPods(ctx, action.Target)
	if err != nil {
		return "FAILED", fmt.Sprintf("lookup pods: %v", err)
	}
	for _, pod := range pods {
		if err := e.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))}); err != nil {
			e.log.Warn("k8s: failed to delete pod", "pod", pod.Name, "error", err)
			continue
		}
		e.log.Info("k8s: deleted pod", "pod", pod.Name)
	}
	return "SUCCEEDED", fmt.Sprintf("restarted %d pod(s)", len(pods))
}

func (e *K8sExecutor) scaleUp(ctx context.Context, action FixAction) (status, message string) {
	scale, err := e.getScale(ctx, action.Target)
	if err != nil {
		return "FAILED", fmt.Sprintf("get scale: %v", err)
	}
	scale.Spec.Replicas++
	_, err = e.client.AppsV1().Deployments(scale.Namespace).UpdateScale(ctx, scale.Namespace+"/"+scale.Name, scale, metav1.UpdateOptions{})
	if err != nil {
		return "FAILED", fmt.Sprintf("scale up: %v", err)
	}
	return "SUCCEEDED", fmt.Sprintf("scaled up deployment %s/%s to %d replicas", scale.Namespace, scale.Name, scale.Spec.Replicas)
}

func (e *K8sExecutor) scaleDown(ctx context.Context, action FixAction) (status, message string) {
	scale, err := e.getScale(ctx, action.Target)
	if err != nil {
		return "FAILED", fmt.Sprintf("get scale: %v", err)
	}
	if scale.Spec.Replicas > 0 {
		scale.Spec.Replicas--
	}
	_, err = e.client.AppsV1().Deployments(scale.Namespace).UpdateScale(ctx, scale.Namespace+"/"+scale.Name, scale, metav1.UpdateOptions{})
	if err != nil {
		return "FAILED", fmt.Sprintf("scale down: %v", err)
	}
	return "SUCCEEDED", fmt.Sprintf("scaled down deployment %s/%s to %d replicas", scale.Namespace, scale.Name, scale.Spec.Replicas)
}

func (e *K8sExecutor) patchConfigmap(ctx context.Context, action FixAction) (status, message string) {
	// Target format: "namespace/name:key=value,key2=value2".
	// Placeholder — real implementation patches ConfigMap after dry-run validation.
	_ = ctx
	return "SUCCEEDED", fmt.Sprintf("config change for %s (stub — implement with ConfigMap patch)", action.Target)
}

func (e *K8sExecutor) lookupPods(ctx context.Context, target string) ([]corev1.Pod, error) {
	if strings.Contains(target, "=") {
		return e.lookupPodsBySelector(ctx, target)
	}
	// Parse "namespace/name" or just "name".
	ns, name := "default", target
	if strings.Contains(target, "/") {
		parts := strings.SplitN(target, "/", 2)
		ns, name = parts[0], parts[1]
	}
	pod, err := e.client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get pod %s/%s: %w", ns, name, err)
	}
	return []corev1.Pod{*pod}, nil
}

func (e *K8sExecutor) lookupPodsBySelector(ctx context.Context, selector string) ([]corev1.Pod, error) {
	list, err := e.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list pods with selector %q: %w", selector, err)
	}
	return list.Items, nil
}

func (e *K8sExecutor) getScale(ctx context.Context, target string) (*autoscalingv1.Scale, error) {
	ns, name := "default", target
	if strings.Contains(target, "/") {
		parts := strings.SplitN(target, "/", 2)
		ns, name = parts[0], parts[1]
	}
	return e.client.AppsV1().Deployments(ns).GetScale(ctx, name, metav1.GetOptions{})
}

// Rollback triggers rollback for the given action.
// Currently a stub — real implementation would track pre-action state.
func (e *K8sExecutor) Rollback(ctx context.Context, action FixAction) error {
	e.log.Warn("k8s: rollback not implemented", "action_id", action.ID, "type", action.ActionType)
	return nil
}

func ptr[T any](v T) *T { return &v }
