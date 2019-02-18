package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/util/errors"
	k8s "k8s.io/client-go/kubernetes"
)

// nodeManager manages nodes of the kubernets cluster
type nodeManager struct {
	client k8s.Interface
}

type Node interface {
	Cordon() error
	Uncordon() error
	IsCordoned() (bool, error)

	Drain() error
}

type NodeManager interface {
	Node(name string) Node
}

// NewNodeManager creates a new node manager that connects to the
// cluster using the given client
func NewNodeManager(client k8s.Interface) (NodeManager, error) {
	return &nodeManager{
		client: client,
	}, nil
}

type nodePatchUnschedulable struct {
	Spec struct {
		Unschedulable bool `json:"unschedulable"`
	} `json:"spec,omitempty"`
}

func newNodePatchUnschedulable(state bool) nodePatchUnschedulable {
	var patch nodePatchUnschedulable
	patch.Spec.Unschedulable = state
	return patch
}

type node struct {
	manager *nodeManager
	name    string
}

// Node returns the control interface for the given node
func (nm *nodeManager) Node(name string) Node {
	return &node{
		manager: nm,
		name:    name,
	}
}

func (n *node) Cordon() error {
	return n.manager.CordonNode(n.name)
}

func (n *node) Uncordon() error {
	return n.manager.UncordonNode(n.name)
}

func (n *node) IsCordoned() (bool, error) {
	return n.manager.IsNodeCordoned(n.name)
}

func (n *node) Drain() error {
	return n.manager.DrainNode(n.name)
}

// PatchNodeUnschedulable patches the Spec.Unschedulable field of a node
func (nm *nodeManager) PatchNodeUnschedulable(name string, state bool) error {
	bytes, err := json.Marshal(newNodePatchUnschedulable(state))
	if err != nil {
		return errors.Wrap(err, "failed to patch node")
	}

	_, err = nm.client.CoreV1().Nodes().Patch(name, k8stypes.StrategicMergePatchType, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to patch node")
	}

	return nil
}

// GetNodePods returns a list of Pods running on the given node
func (nm *nodeManager) GetNodePods(name string) ([]v1.Pod, error) {

	list, err := nm.client.CoreV1().Pods("").List(metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + name,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pod list")
	}

	return list.Items, nil
}

// UncordonNode marks the node as schedulable
func (nm *nodeManager) UncordonNode(name string) error {
	return nm.PatchNodeUnschedulable(name, false)
}

// CordonNode marks the node as unschedulable
func (nm *nodeManager) CordonNode(name string) error {
	return nm.PatchNodeUnschedulable(name, true)
}

func (nm *nodeManager) IsNodeCordoned(name string) (bool, error) {

	node, err := nm.client.CoreV1().Nodes().Get(name, metav1.GetOptions{})
	if err != nil {
		return false, errors.Wrap(err, "failed to get node")
	}

	return node.Spec.Unschedulable, nil
}

// DrainNode drains the given node by cordon it and
// evicting all pods.
func (nm *nodeManager) DrainNode(name string) error {

	// Drain node does the following:
	// 	CordonNode
	// 	Evict all Pods on that node
	if err := nm.CordonNode(name); err != nil {
		return err
	}

	/*pods, err := nm.GetNodePods(name)
	if err != nil {
		return err
	}

	/*for _, pod := range pods {
		if err := EvictPod(nm.client, pod.Namespace, pod.Name); err != nil {
			return err
		}
	}*/

	return nil
}

func patchNodeUnschedulable(client k8s.Interface, name string, state bool) error {
	bytes, err := json.Marshal(newNodePatchUnschedulable(state))
	if err != nil {
		return errors.Wrap(err, "failed to patch node")
	}

	_, err = client.CoreV1().Nodes().Patch(name, k8stypes.StrategicMergePatchType, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to patch node")
	}

	return nil
}

func cordonNode(client k8s.Interface, name string) error {
	log.Printf("Cordon node %s", name)
	return patchNodeUnschedulable(client, name, true)
}

func uncordonNode(client k8s.Interface, name string) error {
	log.Printf("Uncordon node %s", name)
	return patchNodeUnschedulable(client, name, false)
}

func getNodePods(client k8s.Interface, name string) ([]v1.Pod, error) {

	list, err := client.CoreV1().Pods("").List(metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + name,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pod list")
	}

	return list.Items, nil
}

func drainNode(ctx context.Context, client k8s.Interface, name string, options *metav1.DeleteOptions) error {
	if err := cordonNode(client, name); err != nil {
		return errors.Wrap(err, "failed to drain node")
	}

	pods, err := getNodePods(client, name)
	if err != nil {
		return errors.Wrap(err, "failed to drain node")
	}

	errorChannel := make(chan error)
	defer close(errorChannel)
	var waitGroup sync.WaitGroup
	var errorList []error
	var evictions int

	for _, pod := range pods {

		// Ignore daemonsets
		controller := metav1.GetControllerOf(&pod)
		if controller != nil && controller.Kind == "DaemonSet" {
			continue
		}
		// Ignore mirror pods
		if _, found := pod.ObjectMeta.Annotations[v1.MirrorPodAnnotationKey]; found {
			continue
		}

		evictions++
		waitGroup.Add(1)
		go func(pod v1.Pod) {
			defer waitGroup.Done()
			errorChannel <- errors.Wrap(
				evictPod(ctx, client, pod.GetName(), pod.GetNamespace(), options),
				"failed to drain node",
			)
		}(pod)
	}

	// Check for errors
	for evictions > 0 {
		select {
		case err := <-errorChannel:
			evictions--
			if err != nil {
				errorList = append(errorList, err)
			}
		}
	}

	// should return immediately
	waitGroup.Wait()

	if len(errorList) != 0 {
		return k8serrors.NewAggregate(errorList)
	}
	return nil
}
