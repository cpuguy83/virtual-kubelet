// Copyright © 2017 The virtual-kubelet authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package root

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/virtual-kubelet/virtual-kubelet/cmd/virtual-kubelet/internal/provider"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/internal/manager"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

// NewCommand creates a new top-level command.
// This command is used to start the virtual-kubelet daemon
func NewCommand(ctx context.Context, name string, s *provider.Store, c Opts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   name,
		Short: name + " provides a virtual kubelet interface for your kubernetes cluster.",
		Long: name + ` implements the Kubelet interface with a pluggable
backend implementation allowing users to create kubernetes nodes without running the kubelet.
This allows users to schedule kubernetes workloads on nodes that aren't running Kubernetes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRootCommand(ctx, s, c)
		},
	}

	installFlags(cmd.Flags(), &c)
	return cmd
}

func runRootCommand(ctx context.Context, s *provider.Store, c Opts) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if ok := provider.ValidOperatingSystems[c.OperatingSystem]; !ok {
		return errdefs.InvalidInputf("operating system %q is not supported", c.OperatingSystem)
	}

	if c.PodSyncWorkers == 0 {
		return errdefs.InvalidInput("pod sync workers must be greater than 0")
	}

	var taint *corev1.Taint
	if !c.DisableTaint {
		var err error
		taint, err = getTaint(c)
		if err != nil {
			return err
		}
	}

	client, err := nodeutil.ClientsetFromEnv(c.KubeConfigPath)
	if err != nil {
		return err
	}

	// Create a shared informer factory for Kubernetes pods in the current namespace (if specified) and scheduled to the current node.
	podInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(
		client,
		c.InformerResyncPeriod,
		kubeinformers.WithNamespace(c.KubeNamespace),
		nodeutil.PodInformerFilter(c.NodeName),
	)
	podInformer := podInformerFactory.Core().V1().Pods()

	// Create another shared informer factory for Kubernetes secrets and configmaps (not subject to any selectors).
	scmInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(client, c.InformerResyncPeriod)
	// Create a secret informer and a config map informer so we can pass their listers to the resource manager.
	secretInformer := scmInformerFactory.Core().V1().Secrets()
	configMapInformer := scmInformerFactory.Core().V1().ConfigMaps()
	serviceInformer := scmInformerFactory.Core().V1().Services()

	rm, err := manager.NewResourceManager(podInformer.Lister(), secretInformer.Lister(), configMapInformer.Lister(), serviceInformer.Lister())
	if err != nil {
		return errors.Wrap(err, "could not create resource manager")
	}

	if err := setupTracing(ctx, c); err != nil {
		return err
	}

	initConfig := provider.InitConfig{
		ConfigPath:        c.ProviderConfigPath,
		NodeName:          c.NodeName,
		OperatingSystem:   c.OperatingSystem,
		ResourceManager:   rm,
		DaemonPort:        c.ListenPort,
		InternalIP:        os.Getenv("VKUBELET_POD_IP"),
		KubeClusterDomain: c.KubeClusterDomain,
	}

	pInit := s.Get(c.Provider)
	if pInit == nil {
		return errors.Errorf("provider %q not found", c.Provider)
	}

	p, err := pInit(initConfig)
	if err != nil {
		return errors.Wrapf(err, "error initializing provider %s", c.Provider)
	}

	ctx = log.WithLogger(ctx, log.G(ctx).WithFields(log.Fields{
		"provider":         c.Provider,
		"operatingSystem":  c.OperatingSystem,
		"node":             c.NodeName,
		"watchedNamespace": c.KubeNamespace,
	}))

	var pmh api.PodStatsSummaryHandlerFunc
	if pm, ok := p.(provider.PodMetricsProvider); ok {
		pmh = pm.GetStatsSummary
	}

	tlsConfig, err := loadTLSConfig(os.Getenv("APISERVER_CERT_LOCATION"), os.Getenv("APISERVER_KEY_LOCATION"))
	if err != nil {
		return err
	}
	l, err := tls.Listen("tcp", fmt.Sprintf(":%d", c.ListenPort), tlsConfig)
	if err != nil {
		return errors.Wrap(err, "error setting up listener")
	}

	ac, err := api.NewController(api.ControllerConfig{
		PodListener: l,
		EnableDebug: true,
		PodHandler: api.PodHandlerConfig{
			RunInContainer:   p.RunInContainer,
			GetContainerLogs: p.GetContainerLogs,
			GetPodsFromKubernetes: func(context.Context) ([]*corev1.Pod, error) {
				return rm.GetPods(), nil
			},
			GetPods:               p.GetPods,
			GetStatsSummary:       pmh,
			StreamIdleTimeout:     c.StreamIdleTimeout,
			StreamCreationTimeout: c.StreamCreationTimeout,
		},
	})
	if err != nil {
		return errors.Wrap(err, "error creating api controller")
	}

	pNode := NodeFromProvider(ctx, c.NodeName, taint, p, c.Version)
	np := node.NewNaiveNodeProvider()

	nc, err := node.NewNodeController(np, pNode, client.CoreV1().Nodes(),
		node.WithNodeStatusUpdateErrorHandler(func(ctx context.Context, err error) error {
			if !k8serrors.IsNotFound(err) {
				return err
			}

			log.G(ctx).Debug("node not found")
			newNode := pNode.DeepCopy()
			newNode.ResourceVersion = ""
			_, err = client.CoreV1().Nodes().Create(ctx, newNode, metav1.CreateOptions{})
			if err != nil {
				return err
			}
			log.G(ctx).Debug("created new node")
			return nil
		}),
		func(nc *node.NodeController) error {
			if c.EnableNodeLease {
				return node.WithNodeEnableLeaseV1Beta1(nodeutil.NodeLeaseV1Beta1Client(client), nil)(nc)
			}
			return nil
		},
	)

	eb := record.NewBroadcaster()
	pc, err := node.NewPodController(node.PodControllerConfig{
		PodClient:         client.CoreV1(),
		PodInformer:       podInformer,
		ConfigMapInformer: configMapInformer,
		ServiceInformer:   serviceInformer,
		SecretInformer:    secretInformer,
		Provider:          p,
		EventRecorder:     eb.NewRecorder(scheme.Scheme, corev1.EventSource{Component: path.Join(pNode.Name, "pod-controller")}),
	})
	if err != nil {
		return errors.Wrap(err, "error creating pod controller")
	}

	m, err := node.NewControllerManager(node.ControllerManagerConfig{
		APIController:        ac,
		NodeController:       nc,
		PodController:        pc,
		PodControllerWorkers: c.PodSyncWorkers,
		StartupTimeout:       c.StartupTimeout,
	})

	if err != nil {
		return err
	}

	podInformerFactory.Start(ctx.Done())
	scmInformerFactory.Start(ctx.Done())
	defer eb.StartLogging(log.G(ctx).Infof).Stop()
	defer eb.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: client.CoreV1().Events(corev1.NamespaceAll)}).Stop()

	go m.Run(ctx)

	// Wait for the controller manager (and thus all controllers) to be ready.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.Ready():
	case <-m.Done():
		return m.Err()
	}

	// Now set the node as ready in the API server
	//
	// TODO: Can/should this be moved to the controller manager somehow?
	nodeutil.SetNodeReady(pNode)
	if err := np.UpdateStatus(ctx, pNode); err != nil {
		return err
	}

	log.G(ctx).Info("Initialized")

	select {
	case <-ctx.Done():
	case <-m.Done():
		return m.Err()
	}
	return nil
}
