package main

import (
	"context"
	"time"

	arangoapi "github.com/arangodb/kube-arangodb/pkg/apis/deployment/v1alpha"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ActionType string

const (
	ActionTypeCreateDeployment ActionType = "CreateDeployment"
	ActionTypeDeleteDeployment ActionType = "DeleteDeployment"
	ActionTypeDeployOperator   ActionType = "DeployOperator"
	ActionTypeDeleteOperator   ActionType = "DeleteOperator"

	ActionTypeDeletePod ActionType = "DeletePod"
	ActionTypeEvictPod  ActionType = "EvictPod"
	ActionTypeDrainNode ActionType = "DrainNode"

	ActionDeletePVC ActionType = "DeletePVC"

	ActionKillNode ActionType = "KillNode"
)

type ActionCreateDeploymentDescription struct {
	Spec arangoapi.DeploymentSpec
}

type ActionDeployOperatorDescription struct {
	Image string
}

type ActionDeletePodDescription struct {
	Target            PodTarget `json:"target"`
	WaitForCompletion bool      `json:"waitForCompletion"`
}

type ActionDescription struct {
	Type          ActionType    `json:"action"`
	WaitForHealth bool          `json:"waitForHealth"`
	Delay         time.Duration `json:"delay"`

	CreateDeployment *ActionCreateDeploymentDescription `json:inline`
	DeployOperator   *ActionDeployOperatorDescription   `json:inline`
}

type ActionInterface interface {
	Nodes() NodeManager
	Pods() PodManager
	Deployment() DeploymentManager
	ErrorChannel() chan error
}

type Action interface {
	Run(ctx context.Context, iface ActionInterface) error
}

type ActionDescriptionList []ActionDescription

type ActionScript struct {
	Cluster ClusterConfig         `json:"clusterConfig"`
	Actions ActionDescriptionList `json:"actions"`
}

func NewAction(desc ActionDescription) (Action, error) {
	return nil, nil
}

type actionDeletePod struct {
	target            PodTarget
	waitForCompletion bool
}

func newActionDeletePod(desc ActionDeletePodDescription) Action {
	return &actionDeletePod{
		target:            desc.Target,
		waitForCompletion: desc.WaitForCompletion,
	}
}

func newErrorChannelOrDefault(iface ActionInterface, new bool) chan error {
	if new {
		return make(chan error)
	}

	return iface.ErrorChannel()
}

func waitForCompletion(ctx context.Context, completed <-chan error) error {
	select {
	case err, ok := <-completed:
		if ok {
			return err
		}
		return errors.New("Error channel closed unexpectedly")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *actionDeletePod) Run(ctx context.Context, iface ActionInterface) error {

	options := metav1.DeleteOptions{}
	channel := newErrorChannelOrDefault(iface, a.waitForCompletion)
	if err := iface.Pods().Target(a.target).Delete(ctx, channel, &options); err != nil {
		return nil
	}

	if a.waitForCompletion {
		defer close(channel)
		waitForCompletion(ctx, channel)
	}

	return nil
}
