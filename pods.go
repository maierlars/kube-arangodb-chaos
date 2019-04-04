package main

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	policy "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fields "k8s.io/apimachinery/pkg/fields"
	watch "k8s.io/apimachinery/pkg/watch"
	k8s "k8s.io/client-go/kubernetes"
	api "k8s.io/kubernetes/pkg/apis/core"
)

type PodGroup string

const (
	PodGroupOperator    PodGroup = "Operator"
	PodGroupAgent       PodGroup = "AgentLeader"
	PodGroupCoordinator PodGroup = "Coordinator"
	PodGroupDBServer    PodGroup = "DBServer"
)

type PodTarget struct {
	Group    PodGroup
	IsLeader bool
	IsReady  bool
}

type Pod interface {
	// Evict creates a Eviction for the Pod
	Evict(ctx context.Context, completion chan<- error, options *metav1.DeleteOptions) error
	// Delete deletes the pod
	Delete(ctx context.Context, completion chan<- error, options *metav1.DeleteOptions) error

	// Node returns the Node of this Pod
	Node() (Node, error)
}

type PodManager interface {
	// Pod returns the pod with the given name
	Pod(name string) Pod

	// Target returns a pod satisfying the given target constraints or nil
	// if there is no such pod.
	Target(target PodTarget) Pod
}

// evictPod creates a Eviction resource for the given pod and waits for the pod to be deleted
// Ignores if pod is not found
func evictPod(ctx context.Context, client k8s.Interface, name, namespace string, options *metav1.DeleteOptions) error {

	const (
		EvictionKind       = "Eviction"
		PolicyGroupVersion = "policy/v1beta1"
	)

	eviction := &policy.Eviction{
		TypeMeta: metav1.TypeMeta{
			APIVersion: PolicyGroupVersion,
			Kind:       EvictionKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		DeleteOptions: options,
	}

	// Receive events for the Pod before evicting it
	watcher, err := client.CoreV1().Pods(namespace).Watch(metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(api.ObjectNameField, name).String(),
	})
	if apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "failed to watch pod")
	}
	defer watcher.Stop()

	// try multiple times to evict the pod
	for {
		if err := client.CoreV1().Pods(namespace).Evict(eviction); err == nil {
			log.Printf("Created Eviction for Pod %s/%s", namespace, name)
			break
		} else if apierrors.IsNotFound(err) {
			return nil
		} else if !apierrors.IsTooManyRequests(err) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	for {
		select {
		case ev, ok := <-watcher.ResultChan():
			if !ok {
				return errors.New("watch channel closed")
			}
			if ev.Type == watch.Deleted {
				log.Printf("%s/%s evicted", namespace, name)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DeletePod delete the given pod
func deletePod(ctx context.Context, client k8s.Interface, namespace, name string, options *metav1.DeleteOptions) error {

	// Receive events for the Pod before evicting it
	watcher, err := client.CoreV1().Pods(namespace).Watch(metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(api.ObjectNameField, name).String(),
	})
	if err != nil {
		return errors.Wrap(err, "failed to watch pod")
	}
	defer watcher.Stop()

	log.Printf("Deleting Pod %s/%s", namespace, name)
	if err := client.CoreV1().Pods(namespace).Delete(name, options); err != nil {
		return err
	}

	for {
		select {
		case ev, ok := <-watcher.ResultChan():
			if !ok {
				return errors.New("watch channel closed")
			}
			if ev.Type == watch.Deleted {
				log.Printf("%s/%s deleted", namespace, name)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
