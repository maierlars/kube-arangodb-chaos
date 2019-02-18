package main

/*
type ExecutionContext interface {
	Nodes() NodeManager
	Pods() PodManager
}

type Executor struct {
}

func CreateAction(desc ActionDescription) (Action, error) {
	return nil, nil
}

func (e *Executor) Run(ctx context.Context, script ActionDescriptionList) error {

	for _, desc := range script {
		action, err := CreateAction(desc)
		if err != nil {
			return err
		}

		log.Printf("Starting action %s", desc.Type)
		if err := action.Start(); err != nil {
			return err
		}

		if desc.WaitForCompletion {
			log.Printf("Waiting for action completion")
		waitForCompletionLoop:
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(desc.Delay):
					progress, err := action.CheckProgress()
					if err != nil {
						return err
					}

					if progress.IsCompleted() {
						break waitForCompletionLoop
					}

					if progress.IsFailed() {
						return errors.New("Action failed")
					}
				}
			}
		} else {
			time.Sleep(desc.Delay)
		}

		if desc.WaitForHealth {
			log.Printf("Waiting for cluster health")

			timeout, cancel := context.WithTimeout(ctx, 2*time.Minute)

		waitForHealthLoop:
			for {
				select {
				case <-timeout.Done():
					cancel()
					return errors.Wrap(ctx.Err(), "waiting for cluster health failed")
				case <-time.After(20 * time.Second):
					// check cluster health
					cancel()
					break waitForHealthLoop
				}
			}
		}
	}

	return nil
}
*/
