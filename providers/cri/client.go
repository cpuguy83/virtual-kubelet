package cri

import (
	"fmt"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	"golang.org/x/net/context"
	criapi "k8s.io/kubernetes/pkg/kubelet/apis/cri/runtime/v1alpha2"
)

// Call RunPodSandbox on the CRI client
func runPodSandbox(ctx context.Context, client criapi.RuntimeServiceClient, config *criapi.PodSandboxConfig) (string, error) {
	request := &criapi.RunPodSandboxRequest{Config: config}
	log.G(ctx).Debugf("RunPodSandboxRequest: %v", request)
	r, err := client.RunPodSandbox(context.Background(), request)
	log.G(ctx).Debugf("RunPodSandboxResponse: %v", r)
	if err != nil {
		return "", err
	}
	log.G(ctx).WithField("sandbox.ID", r.PodSandboxId).Debug("Created pod sandbox")
	return r.PodSandboxId, nil
}

// Call StopPodSandbox on the CRI client
func stopPodSandbox(ctx context.Context, client criapi.RuntimeServiceClient, id string) error {
	if id == "" {
		return fmt.Errorf("ID cannot be empty")
	}
	request := &criapi.StopPodSandboxRequest{PodSandboxId: id}
	log.G(ctx).Debugf("StopPodSandboxRequest: %v", request)
	r, err := client.StopPodSandbox(context.Background(), request)
	log.G(ctx).Debugf("StopPodSandboxResponse: %v", r)
	if err != nil {
		return err
	}

	log.G(ctx).WithField("sandbox.ID", id).Debug("Stopped sandbox")
	return nil
}

// Call RemovePodSandbox on the CRI client
func removePodSandbox(ctx context.Context, client criapi.RuntimeServiceClient, id string) error {
	if id == "" {
		return fmt.Errorf("ID cannot be empty")
	}
	request := &criapi.RemovePodSandboxRequest{PodSandboxId: id}
	log.G(ctx).Debugf("RemovePodSandboxRequest: %v", request)
	r, err := client.RemovePodSandbox(context.Background(), request)
	log.G(ctx).Debugf("RemovePodSandboxResponse: %v", r)
	if err != nil {
		return err
	}
	log.G(ctx).WithField("sandbox.ID", id).Debug("Removed sandbox")
	return nil
}

// Call ListPodSandbox on the CRI client
func getPodSandboxes(ctx context.Context, client criapi.RuntimeServiceClient) ([]*criapi.PodSandbox, error) {
	filter := &criapi.PodSandboxFilter{}
	request := &criapi.ListPodSandboxRequest{
		Filter: filter,
	}

	log.G(ctx).Debugf("ListPodSandboxRequest: %v", request)
	r, err := client.ListPodSandbox(context.Background(), request)

	log.G(ctx).Debugf("ListPodSandboxResponse: %v", r)
	if err != nil {
		return nil, err
	}
	return r.GetItems(), err
}

// Call PodSandboxStatus on the CRI client
func getPodSandboxStatus(ctx context.Context, client criapi.RuntimeServiceClient, psId string) (*criapi.PodSandboxStatus, error) {
	if psId == "" {
		return nil, fmt.Errorf("Pod ID cannot be empty in GPSS")
	}

	request := &criapi.PodSandboxStatusRequest{
		PodSandboxId: psId,
		Verbose:      false,
	}

	log.G(ctx).Debugf("PodSandboxStatusRequest: %v", request)
	r, err := client.PodSandboxStatus(context.Background(), request)
	log.G(ctx).Debugf("PodSandboxStatusResponse: %v", r)
	if err != nil {
		return nil, err
	}

	return r.Status, nil
}

// Call CreateContainer on the CRI client
func createContainer(ctx context.Context, client criapi.RuntimeServiceClient, config *criapi.ContainerConfig, podConfig *criapi.PodSandboxConfig, pId string) (string, error) {
	request := &criapi.CreateContainerRequest{
		PodSandboxId:  pId,
		Config:        config,
		SandboxConfig: podConfig,
	}
	log.G(ctx).Debugf("CreateContainerRequest: %v", request)
	r, err := client.CreateContainer(context.Background(), request)
	log.G(ctx).Debugf("CreateContainerResponse: %v", r)
	if err != nil {
		return "", err
	}
	log.G(ctx).WithField("container.ID", r.ContainerId).Debug("Container created")
	return r.ContainerId, nil
}

// Call StartContainer on the CRI client
func startContainer(ctx context.Context, client criapi.RuntimeServiceClient, cId string) error {
	if cId == "" {
		return fmt.Errorf("ID cannot be empty")
	}
	request := &criapi.StartContainerRequest{
		ContainerId: cId,
	}
	log.G(ctx).Debugf("StartContainerRequest: %v", request)
	r, err := client.StartContainer(context.Background(), request)
	log.G(ctx).Debugf("StartContainerResponse: %v", r)
	if err != nil {
		return err
	}
	log.G(ctx).WithField("container.ID", cId).Debug("Container started")
	return nil
}

// Call ContainerStatus on the CRI client
func getContainerCRIStatus(ctx context.Context, client criapi.RuntimeServiceClient, cId string) (*criapi.ContainerStatus, error) {
	if cId == "" {
		return nil, fmt.Errorf("Container ID cannot be empty in GCCS")
	}

	request := &criapi.ContainerStatusRequest{
		ContainerId: cId,
		Verbose:     false,
	}
	log.G(ctx).Debugf("ContainerStatusRequest: %v", request)
	r, err := client.ContainerStatus(ctx, request)
	log.G(ctx).Debugf("ContainerStatusResponse: %v", r)
	if err != nil {
		return nil, err
	}

	return r.Status, nil
}

// Call ListContainers on the CRI client
func getContainersForSandbox(ctx context.Context, client criapi.RuntimeServiceClient, psId string) ([]*criapi.Container, error) {
	filter := &criapi.ContainerFilter{}
	filter.PodSandboxId = psId
	request := &criapi.ListContainersRequest{
		Filter: filter,
	}
	log.G(ctx).Debugf("ListContainerRequest: %v", request)
	r, err := client.ListContainers(context.Background(), request)
	log.G(ctx).Debugf("ListContainerResponse: %v", r)
	if err != nil {
		return nil, err
	}
	return r.Containers, nil
}

// Pull and image on the CRI client and return the image ref
func pullImage(ctx context.Context, client criapi.ImageServiceClient, image string) (string, error) {
	request := &criapi.PullImageRequest{
		Image: &criapi.ImageSpec{
			Image: image,
		},
	}
	log.G(ctx).Debugf("PullImageRequest: %v", request)
	r, err := client.PullImage(context.Background(), request)
	log.G(ctx).Debugf("PullImageResponse: %v", r)
	if err != nil {
		return "", err
	}

	return r.ImageRef, nil
}
