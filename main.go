package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var namespace string
var logpath string

func main() {

	rand.Seed(time.Now().Unix())

	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	)
	config, err := kubeConfig.ClientConfig()
	if err != nil {
		// Do something
	}

	client, err := k8s.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	startTime := time.Now().UTC().Format(time.RFC3339)
	log.Printf("Starting k8s chaos agent, %s", startTime)

	/*api, err := apiextension.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	arango, err := arangoapi.NewForConfig(config)
	if err != nil {
		panic(err)
	}*/

	nodes, err := client.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		log.Fatalf("failed to obtain node list: %s", err.Error())
	}

	var usableNodes []string

	for _, node := range nodes.Items {

		if node.Spec.Unschedulable {
			log.Printf("Can not use node %s, unschedulable", node.GetName())
			continue
		}

		if len(node.Spec.Taints) > 0 {
			log.Printf("Can not use node %s, tainted", node.GetName())
			continue
		}

		ready := false
		for _, cond := range node.Status.Conditions {
			if cond.Type == v1.NodeReady {
				ready = true
				break
			}
		}

		if !ready {
			log.Printf("Can not use node %s, not ready", node.GetName())
			continue
		}

		log.Printf("Using node %s", node.GetName())
		usableNodes = append(usableNodes, node.GetName())
	}

	/*ctx, cancel := context.WithTimeout(context.Background(), 22*time.Minute)
	defer cancel()*/
	ctx := context.Background()
	logger, err := NewPodLogger(ctx, "default", "logs/"+startTime+"/pods", client)
	if err != nil {
		log.Fatalf("Failed to create pod logger: %s", err.Error())
	}

chaos:
	for {
		select {
		case <-time.After(5 * time.Second):

			switch rand.Intn(6) {
			case 0, 1, 2:
				pods, err := client.CoreV1().Pods("default").List(metav1.ListOptions{})
				if err != nil {
					log.Printf("Failed to get pod list: %s", err.Error())
				}

				if len(pods.Items) > 0 {

					podid := rand.Intn(len(pods.Items))

					gracePeriod := int64(0)

					if err := deletePod(ctx, client, "default", pods.Items[podid].GetName(), &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
						log.Fatalf("Failed to delete pod: %s", err.Error())
					}
				}
			case 3, 4:
				nodeid := rand.Intn(len(usableNodes))

				log.Printf("Draining node %s", usableNodes[nodeid])
				if err := drainNode(ctx, client, usableNodes[nodeid], &metav1.DeleteOptions{}); err != nil {
					log.Fatalf("Failed to drain node: %s", err.Error())
				}

				log.Printf("Drain completed %s", usableNodes[nodeid])

				time.Sleep(10 * time.Second)

				if err := uncordonNode(client, usableNodes[nodeid]); err != nil {
					log.Fatalf("Failed to uncordon node: %s", err.Error())
				}
			case 5:
				nodeid := rand.Intn(len(usableNodes))

				gracePeriod := int64(10)

				log.Printf("Draining node %s, with force and no grace-period", usableNodes[nodeid])
				if err := drainNode(ctx, client, usableNodes[nodeid], &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
					log.Fatalf("Failed to drain node: %s", err.Error())
				}

				log.Printf("Drain completed %s", usableNodes[nodeid])

				time.Sleep(10 * time.Second)

				if err := uncordonNode(client, usableNodes[nodeid]); err != nil {
					log.Fatalf("Failed to uncordon node: %s", err.Error())
				}
			}

		case <-ctx.Done():
			break chaos
		}
	}

	time.Sleep(5 * time.Second)

	logger.Wait()

	/*

		crds, err := api.ApiextensionsV1beta1().CustomResourceDefinitions().List(metav1.ListOptions{})
		if err != nil {
			panic(err)
		}

		for _, n := range crds.Items {
			fmt.Printf("%s\n", n.GetName())
		}

		depls, err := arango.DatabaseV1alpha().ArangoDeployments("default").List(metav1.ListOptions{})
		if err != nil {
			panic(err)
		}

		for _, n := range depls.Items {
			fmt.Printf("%s\n", n.GetName())
		}*/
}
