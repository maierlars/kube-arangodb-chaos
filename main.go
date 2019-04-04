package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	arangoapi "github.com/arangodb/kube-arangodb/pkg/apis/deployment/v1alpha"
	arangoclient "github.com/arangodb/kube-arangodb/pkg/generated/clientset/versioned/typed/deployment/v1alpha"
	k8sutil "github.com/arangodb/kube-arangodb/pkg/util/k8sutil"
	jg "github.com/dgrijalva/jwt-go"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
)

func retry(ctx context.Context, predicate func() error) error {

	for {
		if err := predicate(); err == nil {
			return nil
		} else {
			log.Printf("Retry predicate returned error: %s", err.Error())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

var (
	namespace    string
	disableChaos bool
	concurrent   int
)

func init() {
	flag.StringVar(&namespace, "namespace", "default", "Namespace to use, must exist")
	flag.BoolVar(&disableChaos, "disable-chaos", false, "Use to disable chaos and only create logs")
	flag.IntVar(&concurrent, "concurrent-chaos", 1, "Amount of concurrent chaos")
}

type cleanupFunc func() error

func main() {

	flag.Parse()

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
	}*/

	arango, err := arangoclient.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	deployments, err := arango.ArangoDeployments(namespace).List(metav1.ListOptions{})
	if err != nil {
		panic(err)
	}

	services, err := client.CoreV1().Services(namespace).List(metav1.ListOptions{})
	if err != nil {
		panic(err)
	}

	deploymentExternalServiceMap := make(map[string]v1.Service)

	for _, deployment := range deployments.Items {
		log.Printf("Found ArangoDB deployment %s", deployment.GetName())
		for _, service := range services.Items {
			if service.GetName() == deployment.GetName()+"-ea" {
				if service.Spec.Type == v1.ServiceTypeLoadBalancer {
					log.Println("Found external access LoadBalancer")
					deploymentExternalServiceMap[deployment.GetName()] = service
				}
			}
		}
	}

	generateJWTForDeployment := func(ctx context.Context, deploymentName string) (string, error) {
		deployment, err := arango.ArangoDeployments(namespace).Get(deploymentName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}

		secret, err := k8sutil.GetTokenSecret(client.CoreV1().Secrets(namespace), deployment.Spec.Authentication.GetJWTSecretName())
		if err != nil {
			return "", err
		}

		token := jg.NewWithClaims(jg.SigningMethodHS256, jg.MapClaims{
			"iss":       "arangodb",
			"server_id": "CHAOS!!!!!",
		})

		// Sign and get the complete encoded token as a string using the secret
		signedToken, err := token.SignedString([]byte(secret))
		if err != nil {
			return "", driver.WithStack(err)
		}

		return signedToken, nil
	}

	waitForDeploymentInSync := func(ctx context.Context, deploymentName string) error {
		deployment, err := arango.ArangoDeployments(namespace).Get(deploymentName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		srv, ok := deploymentExternalServiceMap[deploymentName]
		if !ok {
			return fmt.Errorf("No external access to arangodb deployment %s", deploymentName)
		}

		if len(srv.Status.LoadBalancer.Ingress) == 0 {
			log.Println("No LoadBalancer IP known for " + deploymentName)
			return nil
		}

		hasTLS := deployment.Spec.TLS.GetCASecretName() != "None"

		var config http.ConnectionConfig
		if hasTLS {
			config.Endpoints = []string{"https://" + srv.Status.LoadBalancer.Ingress[0].IP + ":8529"}
			config.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		} else {
			config.Endpoints = []string{"http://" + srv.Status.LoadBalancer.Ingress[0].IP + ":8529"}
		}

		config.DontFollowRedirect = true

		conn, err := http.NewConnection(config)
		if err != nil {
			return err
		}

		token, err := generateJWTForDeployment(ctx, deploymentName)
		if err != nil {
			return err
		}

		dbc, err := driver.NewClient(driver.ClientConfig{
			Connection:     conn,
			Authentication: driver.RawAuthentication("bearer " + token),
		})
		if err != nil {
			return err
		}

		cluster, err := dbc.Cluster(ctx)
		if err != nil {
			return err
		}

		health, err := cluster.Health(ctx)
		if err != nil {
			return err
		}

		for name, m := range health.Health {
			if m.CanBeDeleted {
				continue // Ignore servers that can be deleted
			}
			if m.Status != driver.ServerStatusGood {
				return fmt.Errorf("Member Status not GOOD: %s/%s", deploymentName, name)
			}
		}

		databases, err := dbc.Databases(ctx)
		if err != nil {
			return err
		}

		for _, db := range databases {
			inventory, err := cluster.DatabaseInventory(ctx, db)
			if err != nil {
				return err
			}

			for _, coll := range inventory.Collections {
				if !coll.AllInSync {
					return fmt.Errorf("Collection not ready: %s", coll.Parameters.Name)
				}
			}
		}

		return nil
	}

	waitForDeploymentReady := func(ctx context.Context, deploymentName string) error {
		return retry(ctx, func() error {
			deployment, err := arango.ArangoDeployments(namespace).Get(deploymentName, metav1.GetOptions{})
			if err != nil {
				return err
			}

			if len(deployment.Status.Members.Agents) != deployment.Spec.Agents.GetCount() {
				return fmt.Errorf("Missing agents: %s", deployment.GetName())
			}
			if len(deployment.Status.Members.DBServers) != deployment.Spec.DBServers.GetCount() {
				return fmt.Errorf("Missing dbservers: %s", deployment.GetName())
			}
			if len(deployment.Status.Members.Coordinators) != deployment.Spec.Coordinators.GetCount() {
				return fmt.Errorf("Missing coordinators: %s", deployment.GetName())
			}

			if deployment.Status.Phase != arangoapi.DeploymentPhaseRunning {
				log.Printf("Deployment is not running: %s", deployment.GetName())
				return fmt.Errorf("Deployment is not running: %s", deployment.GetName())
			}

			if err := deployment.Status.Members.ForeachServerGroup(func(group arangoapi.ServerGroup, members arangoapi.MemberStatusList) error {
				for _, member := range members {
					if !member.Conditions.IsTrue(arangoapi.ConditionTypeReady) {
						log.Printf("Member not ready: %s/%s", deployment.GetName(), member.ID)
						return fmt.Errorf("Member not ready: %s", member.ID)
					}

					// Check if the pod exists and is in ready state
					pod, err := client.CoreV1().Pods(namespace).Get(member.PodName, metav1.GetOptions{})
					if err != nil {
						return err
					}

					podReady := false
					for _, cond := range pod.Status.Conditions {
						if cond.Type == v1.PodReady && cond.Status == v1.ConditionTrue {
							podReady = true
						}
					}

					if !podReady {
						return fmt.Errorf("Pod not ready: %s", member.PodName)
					}
				}

				return nil
			}); err != nil {
				return err
			}

			if err := waitForDeploymentInSync(ctx, deployment.GetName()); err != nil {
				return err
			}
			log.Printf("Deployment ready: %s", deployment.GetName())
			return nil
		})
	}

	waitForDeploymentsReady := func(ctx context.Context) error {
		for _, deployment := range deployments.Items {
			if err := waitForDeploymentReady(ctx, deployment.GetName()); err != nil {
				return err
			}
		}

		return nil
	}

	/*waitForDeploymentsInSync := func() {

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
	_, err = NewPodLogger(ctx, namespace, "logs/"+startTime+"/pods", client)
	if err != nil {
		log.Fatalf("Failed to create pod logger: %s", err.Error())
	}

	if disableChaos {
		log.Print("Chaos is disabled")
		for {
			select {
			case <-ctx.Done():
				return
			}
		}
	}

	time.Sleep(10 * time.Second)

	if err := waitForDeploymentsReady(ctx); err != nil {
		log.Fatalf("Deployment not ready: %s", err.Error())
	}
	// Returns a (cleanup, chaos) functions
	generateChaos := func() (func(), func()) {
		switch rand.Intn(11) {
		case 0, 1, 2:
			return nil, func() {
				pods, err := client.CoreV1().Pods(namespace).List(metav1.ListOptions{})
				if err != nil {
					log.Printf("Failed to get pod list: %s", err.Error())
				}

				if len(pods.Items) > 0 {

					podid := rand.Intn(len(pods.Items))

					gracePeriod := int64(0)

					if err := deletePod(ctx, client, namespace, pods.Items[podid].GetName(), &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
						log.Fatalf("Failed to delete pod: %s", err.Error())
					}
				}
			}

		case 3, 4:
			nodeid := rand.Intn(len(usableNodes))
			return func() {
					if err := uncordonNode(client, usableNodes[nodeid]); err != nil {
						log.Fatalf("Failed to uncordon node: %s", err.Error())
					}
				}, func() {

					log.Printf("Draining node %s", usableNodes[nodeid])
					if err := drainNode(ctx, client, usableNodes[nodeid], &metav1.DeleteOptions{}); err != nil {
						log.Fatalf("Failed to drain node: %s", err.Error())
					}

					log.Printf("Drain completed %s", usableNodes[nodeid])
				}

		case 5:
			nodeid := rand.Intn(len(usableNodes))

			return func() {
					if err := uncordonNode(client, usableNodes[nodeid]); err != nil {
						log.Fatalf("Failed to uncordon node: %s", err.Error())
					}
				}, func() {
					gracePeriod := int64(0)

					log.Printf("Draining node %s, with force and no grace-period", usableNodes[nodeid])
					if err := drainNode(ctx, client, usableNodes[nodeid], &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
						log.Fatalf("Failed to drain node: %s", err.Error())
					}

					log.Printf("Drain completed %s", usableNodes[nodeid])
				}
		case 6, 7, 8:
			nodeid := rand.Intn(len(usableNodes))

			return func() {
					if err := uncordonNode(client, usableNodes[nodeid]); err != nil {
						log.Fatalf("Failed to uncordon node: %s", err.Error())
					}
				}, func() {
					gracePeriod := rand.Int63n(200) + 10

					log.Printf("Draining node %s, with grace-period %d", usableNodes[nodeid], gracePeriod)
					if err := drainNode(ctx, client, usableNodes[nodeid], &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
						log.Fatalf("Failed to drain node: %s", err.Error())
					}

					log.Printf("Drain completed %s", usableNodes[nodeid])
				}
		case 9, 10:
			nodeid := rand.Intn(len(usableNodes))

			return func() {
					if err := uncordonNode(client, usableNodes[nodeid]); err != nil {
						log.Fatalf("Failed to uncordon node: %s", err.Error())
					}
				}, func() {
					gracePeriod := int64(0)
					log.Printf("Simulating crash of node %s", usableNodes[nodeid])
					if err := simulateCrashNode(ctx, client, usableNodes[nodeid], &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
						log.Fatalf("Failed to crash node: %s", err.Error())
					}

					log.Printf("Crash completed %s", usableNodes[nodeid])
				}
		}

		return nil, nil
	}

	for {
		var cleanupFuncs []func()
		var wg sync.WaitGroup
		i := 0
		for {
			clean, chaos := generateChaos()

			if clean != nil {
				cleanupFuncs = append(cleanupFuncs, clean)
			}

			wg.Add(1)
			go func() {
				chaos()
				wg.Done()
			}()
			log.Printf("Started chaos")

			i++

			if i == concurrent {
				break
			}

			timeout := rand.Intn(100)
			log.Printf("Waiting %d seconds until next chaos", timeout)

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(timeout) * time.Second):
			}
		}

		wg.Wait()

		for {
			timeout, cancel := context.WithTimeout(ctx, time.Minute)
			if err := waitForDeploymentsReady(timeout); err == nil {
				cancel()
				break
			} else if len(cleanupFuncs) > 0 {
				log.Printf("Deployment not ready, cleanup on chaos: %s", err.Error())
				cf := cleanupFuncs[0]
				cleanupFuncs = cleanupFuncs[1:]
				cf()
			} else {
				log.Printf("Deployment not ready: %s", err.Error())
			}
			cancel()
		}

		for _, cf := range cleanupFuncs {
			cf()
		}
	}

	/*

		crds, err := api.ApiextensionsV1beta1().CustomResourceDefinitions().List(metav1.ListOptions{})
		if err != nil {
			panic(err)
		}

		for _, n := range crds.Items {
			fmt.Printf("%s\n", n.GetName())
		}

		depls, err := arango.DatabaseV1alpha().ArangoDeployments(namespace).List(metav1.ListOptions{})
		if err != nil {
			panic(err)
		}

		for _, n := range depls.Items {
			fmt.Printf("%s\n", n.GetName())
		}*/
}
