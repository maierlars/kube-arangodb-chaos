package main

import k8s "k8s.io/client-go/kubernetes"

type AgencyLogger interface {
	Stop()
}

func NewAgencyLogger(client k8s.Interface, namespace, deployment string) (AgencyLogger, error) {

	// Obtain the JWT Token for the deployment
	// Watch all Pods that belong to the deployment and have role=agent
	return nil, nil
}
