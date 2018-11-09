package vkubelet

import (
	"context"
	"fmt"
	"time"

	"github.com/cpuguy83/strongerrors"
	"github.com/cpuguy83/strongerrors/status/ocstatus"
	pkgerrors "github.com/pkg/errors"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"go.opencensus.io/trace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/staging/src/k8s.io/client-go/util/workqueue"
)

const maxRequeus = 5

func addPodAttributes(span *trace.Span, pod *corev1.Pod) {
	span.AddAttributes(
		trace.StringAttribute("uid", string(pod.GetUID())),
		trace.StringAttribute("namespace", pod.GetNamespace()),
		trace.StringAttribute("name", pod.GetName()),
		trace.StringAttribute("phase", string(pod.Status.Phase)),
		trace.StringAttribute("reason", pod.Status.Reason),
	)
}

func (s *Server) startPodSynchronizer(ctx context.Context, id int) {
	logger := log.G(ctx).WithField("method", "startPodSynchronizer").WithField("podSynchronizer", id)
	logger.Debug("Start pod synchronizer")
	id64 := int64(id)

	for {
		e, quit := s.queue.Get()
		if quit {
			return
		}

		// Let this panic if this is not an event, because this is all that should be there
		// Items in the queue are all managed by us and would be a serious bug if it's an unexected type.
		event := e.(event)

		func() {
			ctx, span := trace.StartSpan(event.ctx, "processEvent")
			defer span.End()

			retries := s.queue.NumRequeues(e)
			span.AddAttributes(
				trace.Int64Attribute("workerID", id64),
				trace.StringAttribute("key", event.key),
				trace.Int64Attribute("retries", int64(retries)),
			)

			defer s.queue.Done(event.key)

			obj, exists, err := s.informer.GetStore().GetByKey(event.key)
			if err != nil {
				span.SetStatus(ocstatus.FromError(err))
				log.G(ctx).WithError(err).Error("Error looking up object from cache store")
				return
			}

			if !exists && obj == nil {
				span.Annotate(nil, "could not find object in store, nothing to do")
				log.G(ctx).Debug("Could not find object in store, nothing to do")
				return
			}

			var pod *corev1.Pod
			switch t := obj.(type) {
			case *corev1.Pod:
				pod = t
			case *cache.DeletedFinalStateUnknown:
				p, ok := t.Obj.(*corev1.Pod)
				if !ok {
					span.SetStatus(trace.Status{Code: trace.StatusCodeInvalidArgument, Message: fmt.Sprintf("unxpected type on event: %T", t.Obj)})
					log.G(ctx).WithField("key", event.key).Errorf("Got unexpected type on event: %T", t.Obj)
					return
				}
				pod = p
			default:
				span.SetStatus(trace.Status{Code: trace.StatusCodeInvalidArgument, Message: fmt.Sprintf("unxpected type on event: %T", obj)})
				log.G(ctx).WithField("key", event.key).Errorf("Got unexpected type on event: %T", obj)
				return
			}

			logger := log.G(ctx).WithField("pod", pod.GetName()).WithField("namespace", pod.GetNamespace()).WithField("retries", retries)
			addPodAttributes(span, pod)

			if !s.resourceManager.UpdatePod(pod) {
				span.Annotate(nil, "no pod update required")
				log.Trace(logger, "nothing to do")
				return
			}

			if err := s.syncPod(ctx, pod); err != nil {
				logger.WithError(err).Error("error syncing pod")
				span.SetStatus(ocstatus.FromError(err))

				if retries > maxRequeus {
					s.queue.Forget(e)
					span.Annotate(nil, "not requeueing")
					log.Trace(logger, "not requeuing")
					return
				}

				log.Trace(logger, "requeueing event")
				s.queue.AddRateLimited(e)
				span.Annotate(nil, "requeued event")
				return
			}

			s.queue.Forget(e)
			span.Annotate(nil, "pod synced")
			log.Trace(logger, "pod synced")
		}()
	}
}

func (s *Server) syncPod(ctx context.Context, pod *corev1.Pod) error {
	ctx, span := trace.StartSpan(ctx, "syncPod")
	defer span.End()
	logger := log.G(ctx).WithField("pod", pod.GetName()).WithField("namespace", pod.GetNamespace())

	addPodAttributes(span, pod)

	if pod.DeletionTimestamp != nil ||
		s.resourceManager.GetPod(pod.GetNamespace(), pod.GetName()) == nil {
		logger.Debugf("Deleting pod")
		if err := s.deletePod(ctx, pod); err != nil {
			span.SetStatus(ocstatus.FromError(err))
			return err
		}
		span.Annotate(nil, "pod deleted")
	} else {
		if pod.Status.Phase == corev1.PodFailed {
			logger.Debug("skipping failed pod")
			span.Annotate(nil, "skipping failed pod")
			return nil
		}

		logger.Debugf("Creating pod")
		if err := s.createPod(ctx, pod); err != nil {
			span.SetStatus(ocstatus.FromError(err))
			return err
		}
		span.Annotate(nil, "pod created")
	}

	return nil
}

func (s *Server) createPod(ctx context.Context, pod *corev1.Pod) error {
	ctx, span := trace.StartSpan(ctx, "createPod")
	defer span.End()
	addPodAttributes(span, pod)

	if err := s.populateEnvironmentVariables(pod); err != nil {
		span.SetStatus(trace.Status{Code: trace.StatusCodeInvalidArgument, Message: err.Error()})
		return err
	}

	logger := log.G(ctx).WithField("pod", pod.GetName()).WithField("namespace", pod.GetNamespace())

	if origErr := s.provider.CreatePod(ctx, pod); origErr != nil {
		podPhase := corev1.PodPending
		if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
			podPhase = corev1.PodFailed
		}

		pod.ResourceVersion = "" // Blank out resource version to prevent object has been modified error
		pod.Status.Phase = podPhase
		pod.Status.Reason = podStatusReasonProviderFailed
		pod.Status.Message = origErr.Error()

		_, err := s.k8sClient.CoreV1().Pods(pod.Namespace).UpdateStatus(pod)
		if err != nil {
			logger.WithError(err).Warn("Failed to update pod status")
		} else {
			span.Annotate(nil, "Updated k8s pod status")
		}

		span.SetStatus(trace.Status{Code: trace.StatusCodeUnknown, Message: origErr.Error()})
		return origErr
	}

	span.Annotate(nil, "Created pod in provider")
	logger.Info("Pod created")

	return nil
}

func (s *Server) deletePod(ctx context.Context, pod *corev1.Pod) error {
	ctx, span := trace.StartSpan(ctx, "deletePod")
	defer span.End()
	addPodAttributes(span, pod)

	var delErr error
	if delErr = s.provider.DeletePod(ctx, pod); delErr != nil {
		if strongerrors.IsNotFound(delErr) {
			span.SetStatus(ocstatus.FromError(delErr))
		}
		span.SetStatus(trace.Status{Code: trace.StatusCodeUnknown, Message: delErr.Error()})
		return delErr
	}
	span.Annotate(nil, "Deleted pod from provider")

	logger := log.G(ctx).WithField("pod", pod.GetName()).WithField("namespace", pod.GetNamespace())
	var grace int64
	if err := s.k8sClient.CoreV1().Pods(pod.GetNamespace()).Delete(pod.GetName(), &metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		if errors.IsNotFound(err) {
			span.Annotate(nil, "Pod does not exist in k8s, nothing to delete")
			return nil
		}

		span.SetStatus(trace.Status{Code: trace.StatusCodeUnknown, Message: err.Error()})
		return fmt.Errorf("Failed to delete kubernetes pod: %s", err)
	}
	span.Annotate(nil, "Deleted pod from k8s")

	s.resourceManager.DeletePod(pod)
	span.Annotate(nil, "Deleted pod from internal state")
	logger.Info("Pod deleted")

	return nil
}

// updatePodStatuses syncs the providers pod status with the kubernetes pod status.
func (s *Server) updatePodStatuses(ctx context.Context) {
	ctx, span := trace.StartSpan(ctx, "updatePodStatuses")
	defer span.End()

	// Update all the pods with the provider status.
	ls := s.informer.GetStore().List()
	span.AddAttributes(trace.Int64Attribute("nPods", int64(len(ls))))

	for _, i := range ls {
		select {
		case <-ctx.Done():
			span.Annotate(nil, ctx.Err().Error())
			return
		default:
		}

		pod, ok := i.(*corev1.Pod)
		if !ok {
			continue
		}

		if err := s.updatePodStatus(ctx, pod); err != nil {
			logger := log.G(ctx).WithField("pod", pod.GetName()).WithField("namespace", pod.GetNamespace()).WithField("status", pod.Status.Phase).WithField("reason", pod.Status.Reason)
			logger.Error(err)
		}
	}
}

func (s *Server) updatePodStatus(ctx context.Context, pod *corev1.Pod) error {
	ctx, span := trace.StartSpan(ctx, "updatePodStatus")
	defer span.End()
	addPodAttributes(span, pod)

	if pod.Status.Phase == corev1.PodSucceeded ||
		pod.Status.Phase == corev1.PodFailed ||
		pod.Status.Reason == podStatusReasonProviderFailed {
		return nil
	}

	status, err := s.provider.GetPodStatus(ctx, pod.Namespace, pod.Name)
	if err != nil {
		span.SetStatus(ocstatus.FromError(err))
		return pkgerrors.Wrap(err, "error retreiving pod status")
	}

	// Update the pod's status
	if status != nil {
		pod.Status = *status
	} else {
		// Only change the status when the pod was already up
		// Only doing so when the pod was successfully running makes sure we don't run into race conditions during pod creation.
		if pod.Status.Phase == corev1.PodRunning || pod.ObjectMeta.CreationTimestamp.Add(time.Minute).Before(time.Now()) {
			// Set the pod to failed, this makes sure if the underlying container implementation is gone that a new pod will be created.
			pod.Status.Phase = corev1.PodFailed
			pod.Status.Reason = "NotFound"
			pod.Status.Message = "The pod status was not found and may have been deleted from the provider"
			for i, c := range pod.Status.ContainerStatuses {
				var startedAt metav1.Time
				if c.State.Running != nil {
					startedAt = c.State.Running.StartedAt
				}
				pod.Status.ContainerStatuses[i].State.Terminated = &corev1.ContainerStateTerminated{
					ExitCode:    -137,
					Reason:      "NotFound",
					Message:     "Container was not found and was likely deleted",
					FinishedAt:  metav1.NewTime(time.Now()),
					StartedAt:   startedAt,
					ContainerID: c.ContainerID,
				}
				pod.Status.ContainerStatuses[i].State.Running = nil
			}
		}
	}

	if _, err := s.k8sClient.CoreV1().Pods(pod.Namespace).UpdateStatus(pod); err != nil {
		span.SetStatus(ocstatus.FromError(err))
		return pkgerrors.Wrap(err, "error while updating pod status in kubernetes")
	}

	span.Annotate([]trace.Attribute{
		trace.StringAttribute("new phase", string(pod.Status.Phase)),
		trace.StringAttribute("new reason", pod.Status.Reason),
	}, "updated pod status in kubernetes")
	return nil
}

type event struct {
	ctx context.Context
	key string
}

// watchForPodEvent waits for pod changes from kubernetes and updates the details accordingly in the local state.
// This returns after a single pod event.
func (s *Server) watchForPodEvent(ctx context.Context) error {
	opts := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", s.nodeName).String(),
	}

	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			if s.informer != nil {
				opts.ResourceVersion = s.informer.GetController().LastSyncResourceVersion()
			}

			return s.k8sClient.Core().Pods(s.namespace).List(opts)
		},

		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			if s.informer != nil {
				opts.ResourceVersion = s.informer.GetController().LastSyncResourceVersion()
			}

			return s.k8sClient.Core().Pods(s.namespace).Watch(opts)
		},
	}

	q := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	s.queue = q

	s.informer = cache.NewSharedInformer(lw, &corev1.Pod{}, time.Minute)
	s.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ctx, span := trace.StartSpan(ctx, "podAdd")
			defer span.End()

			logger := log.G(ctx).WithField("method", "AddFunc")

			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				span.SetStatus(trace.Status{Code: trace.StatusCodeInvalidArgument, Message: err.Error()})
				logger.WithError(err).Error("error generating key for object on pod add handler")
				return
			}

			logger = logger.WithField("key", key)
			ctx = log.WithLogger(ctx, logger)

			logger.Debug("Adding event to queue")
			q.Add(event{ctx: ctx, key: key})
			span.Annotate(nil, "added pod to queue")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ctx, span := trace.StartSpan(ctx, "podUpdate")
			defer span.End()
			logger := log.G(ctx).WithField("method", "UpdateFunc")

			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err != nil {
				span.SetStatus(trace.Status{Code: trace.StatusCodeInvalidArgument, Message: err.Error()})
				log.G(ctx).WithError(err).Error("error generating key for object on pod delete handler")
				return
			}

			logger = logger.WithField("key", key)
			ctx = log.WithLogger(ctx, logger)

			logger.Debug("Adding event to queue")
			q.Add(event{ctx: ctx, key: key})
			span.Annotate(nil, "added pod to queue")
		},
		DeleteFunc: func(obj interface{}) {
			ctx, span := trace.StartSpan(ctx, "podDelete")
			defer span.End()
			logger := log.G(ctx).WithField("method", "DeleteFunc")

			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err != nil {
				span.SetStatus(trace.Status{Code: trace.StatusCodeInvalidArgument, Message: err.Error()})
				logger.Error("error generating key for object on pod deleter handler")
				return
			}

			logger = logger.WithField("key", key)
			ctx = log.WithLogger(ctx, logger)

			logger.Debug("Adding event to queue")
			q.Add(event{ctx: ctx, key: key})
		},
	})

	go s.informer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), s.informer.HasSynced) {
		return pkgerrors.Wrap(ctx.Err(), "error waiting for cache sync")
	}

	for i := 0; i < s.podSyncWorkers; i++ {
		go s.startPodSynchronizer(ctx, i)
	}

	log.G(ctx).Info("Start to run pod cache controller.")

	<-ctx.Done()
	return ctx.Err()
}
