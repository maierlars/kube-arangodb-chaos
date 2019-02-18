package main

import driver "github.com/arangodb/go-driver"

type Deployment interface {
	Delete() error
	Database() driver.Client
}

type DeploymentManager interface {
	Deployment(name string) Deployment
	New() (Deployment, error)
}
