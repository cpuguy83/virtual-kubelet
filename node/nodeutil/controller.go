package nodeutil

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
)

// Node helps manage the startup/shutdown procedure for other controllers.
// It is intended as a convenience to reduce boiler plate code for starting up controllers.
//
// Must be created with constructor `NewNode`.
type Node struct {
	nc *node.NodeController
	pc *node.PodController

	readyCb func(context.Context) error

	ready chan struct{}
	done  chan struct{}
	err   error

	podInformerFactory informers.SharedInformerFactory
	scmInformerFactory informers.SharedInformerFactory
	client             kubernetes.Interface

	eb record.EventBroadcaster
}

// NodeController returns the configured node controller.
func (n *Node) NodeController() *node.NodeController {
	return n.nc
}

// PodController returns the configured pod controller.
func (n *Node) PodController() *node.PodController {
	return n.pc
}

// Run starts all the underlying controllers
func (n *Node) Run(ctx context.Context, workers int) (retErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if n.podInformerFactory != nil {
		go n.podInformerFactory.Start(ctx.Done())
	}
	if n.scmInformerFactory != nil {
		go n.scmInformerFactory.Start(ctx.Done())
	}

	if n.eb != nil {
		n.eb.StartLogging(log.G(ctx).Infof)
		n.eb.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: n.client.CoreV1().Events(v1.NamespaceAll)})
	}

	go n.pc.Run(ctx, workers) // nolint:errcheck

	defer func() {
		cancel()

		<-n.pc.Done()

		n.err = retErr
		close(n.done)
	}()

	select {
	case <-ctx.Done():
		return n.err
	case <-n.pc.Ready():
	case <-n.pc.Done():
		return n.pc.Err()
	}

	go n.nc.Run(ctx) // nolint:errcheck

	defer func() {
		cancel()
		<-n.nc.Done()
	}()

	select {
	case <-ctx.Done():
		n.err = ctx.Err()
		return n.err
	case <-n.nc.Ready():
	case <-n.nc.Done():
		return n.nc.Err()
	}

	if n.readyCb != nil {
		if err := n.readyCb(ctx); err != nil {
			return err
		}
	}
	close(n.ready)

	select {
	case <-n.nc.Done():
		cancel()
		return n.nc.Err()
	case <-n.pc.Done():
		cancel()
		return n.pc.Err()
	}
}

// WaitReady waits for the specified timeout for the controller to be ready.
//
// The timeout is for convenience so the caller doesn't have to juggle an extra context.
func (n *Node) WaitReady(ctx context.Context, timeout time.Duration) error {
	if timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	select {
	case <-n.ready:
		return nil
	case <-n.done:
		return fmt.Errorf("controller exited before ready: %w", n.err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ready returns a channel that will be closed after the controller is ready.
func (n *Node) Ready() <-chan struct{} {
	return n.ready
}

// Done returns a channel that will be closed when the controller has exited.
func (n *Node) Done() <-chan struct{} {
	return n.done
}

// Err returns any error that occurred with the controller.
//
// This always return nil before `<-Done()`.
func (n *Node) Err() error {
	select {
	case <-n.Done():
		return n.err
	default:
		return nil
	}
}

// ProviderConfig holds objects created by NewNodeFromClient that a provider may need to bootstrap itself.
type ProviderConfig struct {
	Pods       corev1listers.PodLister
	ConfigMaps corev1listers.ConfigMapLister
	Secrets    corev1listers.SecretLister
	Services   corev1listers.ServiceLister
	// Hack to allow the provider to set things on the node
	// Since the provider is bootstrapped after the node object is configured
	// Primarily this is due to carry-over from the pre-1.0 interfaces that expect the provider to configure the node.
	Node *v1.Node
}

// NodeOpt is used as functional options when configuring a new node in NewNodeFromClient
type NodeOpt func(c *NodeConfig) error

// NodeConfig is used to hold configuration items for a Node.
// It gets used in conjection with NodeOpt in NewNodeFromClient
type NodeConfig struct {
	// Set the node spec to register with Kubernetes
	NodeSpec v1.Node
	// Set the path to read a kubeconfig from for creating a client.
	// This is ignored when a client is provided to NewNodeFromClient
	KubeconfigPath string
	// Set the period for a full resync for generated client-go informers
	InformerResyncPeriod time.Duration
}

// NewProviderFunc is used from NewNodeFromClient to bootstrap a provider using the client/listers/etc created there.
// If a nil node provider is returned a default one will be used.
type NewProviderFunc func(ProviderConfig) (node.PodLifecycleHandler, node.NodeProvider, error)

// WithNodeConfig returns a NodeOpt which replaces the NodeConfig with the passed in value.
func WithNodeConfig(c NodeConfig) NodeOpt {
	return func(orig *NodeConfig) error {
		*orig = c
		return nil
	}
}

// NewNode calls NewNodeFromClient with a nil client
func NewNode(name string, newProvider NewProviderFunc, opts ...NodeOpt) (*Node, error) {
	return NewNodeFromClient(nil, name, newProvider, opts...)
}

// NewNodeFromClient creates a new node using the provided client and name.
// This is intended for high-level/low boiler-plate usage.
// Use the constructors in the `node` package for lower level configuration.
//
// Some basic values are set for node status, you'll almost certainly want to modify it.
//
// If client is nil, this will construct a client using ClientsetFromEnv
func NewNodeFromClient(client kubernetes.Interface, name string, newProvider NewProviderFunc, opts ...NodeOpt) (*Node, error) {
	cfg := NodeConfig{
		// TODO: this is what was set in the cli code... its not clear what a good value actually is.
		InformerResyncPeriod: time.Minute,
		KubeconfigPath:       os.Getenv("KUBECONFIG"),
		NodeSpec: v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					"type":                   "virtual-kubelet",
					"kubernetes.io/role":     "agent",
					"kubernetes.io/hostname": name,
				},
			},
			Status: v1.NodeStatus{
				Phase: v1.NodePending,
				Conditions: []v1.NodeCondition{
					{Type: v1.NodeReady},
					{Type: v1.NodeDiskPressure},
					{Type: v1.NodeMemoryPressure},
					{Type: v1.NodePIDPressure},
					{Type: v1.NodeNetworkUnavailable},
				},
			},
		},
	}

	for _, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, err
		}
	}

	if client == nil {
		var err error
		client, err = ClientsetFromEnv(cfg.KubeconfigPath)
		if err != nil {
			return nil, errors.Wrap(err, "error creating clientset from env")
		}
	}

	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		cfg.InformerResyncPeriod,
		PodInformerFilter(name),
	)

	scmInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		cfg.InformerResyncPeriod,
	)

	podInformer := podInformerFactory.Core().V1().Pods()
	secretInformer := scmInformerFactory.Core().V1().Secrets()
	configMapInformer := scmInformerFactory.Core().V1().ConfigMaps()
	serviceInformer := scmInformerFactory.Core().V1().Services()

	p, np, err := newProvider(ProviderConfig{
		Pods:       podInformer.Lister(),
		ConfigMaps: configMapInformer.Lister(),
		Secrets:    secretInformer.Lister(),
		Services:   serviceInformer.Lister(),
		Node:       &cfg.NodeSpec,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating provider")
	}

	var readyCb func(context.Context) error
	if np == nil {
		nnp := node.NewNaiveNodeProvider()
		np = nnp

		readyCb = func(ctx context.Context) error {
			setNodeReady(&cfg.NodeSpec)
			err := nnp.UpdateStatus(ctx, &cfg.NodeSpec)
			return errors.Wrap(err, "error marking node as ready")
		}
	}

	nc, err := node.NewNodeController(
		np,
		&cfg.NodeSpec,
		client.CoreV1().Nodes(),
		node.WithNodeEnableLeaseV1(NodeLeaseV1Client(client), node.DefaultLeaseDuration),
	)
	if err != nil {
		return nil, errors.Wrap(err, "error creating node controller")
	}

	eb := record.NewBroadcaster()

	pc, err := node.NewPodController(node.PodControllerConfig{
		PodClient:         client.CoreV1(),
		EventRecorder:     eb.NewRecorder(scheme.Scheme, v1.EventSource{Component: path.Join(name, "pod-controller")}),
		Provider:          p,
		PodInformer:       podInformer,
		SecretInformer:    secretInformer,
		ConfigMapInformer: configMapInformer,
		ServiceInformer:   serviceInformer,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating pod controller")
	}
	return &Node{
		nc:                 nc,
		pc:                 pc,
		readyCb:            readyCb,
		ready:              make(chan struct{}),
		done:               make(chan struct{}),
		eb:                 eb,
		podInformerFactory: podInformerFactory,
		scmInformerFactory: scmInformerFactory,
		client:             client,
	}, nil
}

func setNodeReady(n *v1.Node) {
	n.Status.Phase = v1.NodeRunning
	for i, c := range n.Status.Conditions {
		if c.Type != "Ready" {
			continue
		}

		c.Message = "Kubelet is ready"
		c.Reason = "KubeletReady"
		c.Status = v1.ConditionTrue
		c.LastHeartbeatTime = metav1.Now()
		c.LastTransitionTime = metav1.Now()
		n.Status.Conditions[i] = c
		return
	}
}
