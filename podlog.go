package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	k8s "k8s.io/client-go/kubernetes"
)

type PodLogger struct {
	namespace  string
	logpath    string
	client     k8s.Interface
	group      sync.WaitGroup
	containers map[string]bool
}

func NewPodLogger(ctx context.Context, namespace string, logdir string, client k8s.Interface) (*PodLogger, error) {
	logpath := path.Join(logdir, namespace)

	logger := &PodLogger{
		namespace:  namespace,
		logpath:    logpath,
		client:     client,
		containers: make(map[string]bool),
	}

	// Ensure that the directory exists
	if err := os.MkdirAll(logpath, 0777); err != nil {
		return nil, err
	}

	// Start watching all pods of the namespace
	list, err := client.CoreV1().Pods(namespace).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to watch pods")
	}

	group := &logger.group
	group.Add(1)
	go func() {
		defer group.Done()
		defer list.Stop()
		evchan := list.ResultChan()

		for {
			select {
			case ev := <-evchan:
				if ev.Type == watch.Added || ev.Type == watch.Modified {
					if pod, ok := ev.Object.(*v1.Pod); ok {

						podInitialized := false
						for _, cond := range pod.Status.Conditions {
							if cond.Type == v1.PodInitialized {
								if cond.Status == v1.ConditionTrue {
									podInitialized = true
								}
								break
							}
						}

						if !podInitialized {
							continue
						}

						for i, c := range pod.Spec.Containers {
							podName := fmt.Sprintf("%s_%s_%s", pod.GetName(), c.Name, pod.GetUID())
							if known, ok := logger.containers[podName]; ok && known {
								continue // We are already logging this container
							}

							var state v1.ContainerState
							if i < len(pod.Status.ContainerStatuses) {
								state = pod.Status.ContainerStatuses[i].State
								if state.Running == nil {
									continue
								}
								if state.Waiting != nil {
									log.Printf("Container %s is waiting: %s", c.Name, state.Waiting.Message)
								}
							}

							logFileName := fmt.Sprintf("%s/%s_%s.log", logpath, state.Running.StartedAt.Time.UTC().Format(time.RFC3339), podName)

							logf, err := os.Create(logFileName)
							if err != nil {
								log.Printf("Failed to create local log file: %s", err.Error())
								continue
							}

							stream, err := client.CoreV1().Pods(pod.GetNamespace()).GetLogs(pod.GetName(), &v1.PodLogOptions{Follow: true, Container: c.Name}).Stream()
							if err != nil {
								log.Printf("failed to obtain pod log stream: %s", err.Error())
								continue
							}

							// Start copy function for this container
							logger.containers[podName] = true
							log.Printf("Receiving log for %s/%s/%s %s", pod.GetNamespace(), pod.GetName(), c.Name, pod.GetUID())
							go func(c v1.Container) {
								defer stream.Close()
								if _, err := io.Copy(logf, stream); err != nil {
									log.Printf("Error during log copy: %s", err.Error())
								}
								log.Printf("Log completed for %s/%s/%s %s", pod.GetNamespace(), pod.GetName(), c.Name, pod.GetUID())
							}(c)
						}
					}
				}
			case <-ctx.Done():
				log.Println("PodLogger context done")
				return
			}
		}
	}()

	return logger, nil
}

func (logger *PodLogger) Wait() {
	logger.group.Wait()
}
