package grpctransport

import (
	"context"
	"errors"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"github.com/lihongjie0209/service-registry-service/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registryServer struct {
	registryv1.UnimplementedRegistryServiceServer
	service *registry.Service
	store   *registry.Store
}

func (s *registryServer) RegisterInstance(ctx context.Context, request *registryv1.RegisterInstanceRequest) (*registryv1.RegisterInstanceResponse, error) {
	response, err := s.service.Register(ctx, request)
	return response, registryError(err)
}
func (s *registryServer) RenewLease(ctx context.Context, request *registryv1.RenewLeaseRequest) (*registryv1.RenewLeaseResponse, error) {
	response, err := s.service.Renew(ctx, request)
	return response, registryError(err)
}
func (s *registryServer) DeregisterInstance(ctx context.Context, request *registryv1.DeregisterInstanceRequest) (*registryv1.DeregisterInstanceResponse, error) {
	revision, err := s.service.Deregister(ctx, request)
	if err != nil {
		return nil, registryError(err)
	}
	return &registryv1.DeregisterInstanceResponse{Revision: revision}, nil
}
func (s *registryServer) SetInstanceStatus(ctx context.Context, request *registryv1.SetInstanceStatusRequest) (*registryv1.SetInstanceStatusResponse, error) {
	response, err := s.service.SetStatus(ctx, request)
	return response, registryError(err)
}
func (s *registryServer) GetInstance(ctx context.Context, request *registryv1.GetInstanceRequest) (*registryv1.GetInstanceResponse, error) {
	instance, err := s.service.Get(ctx, request.GetServiceName(), request.GetInstanceId())
	if err != nil {
		return nil, registryError(err)
	}
	return &registryv1.GetInstanceResponse{Instance: instance}, nil
}
func (s *registryServer) ListInstances(ctx context.Context, request *registryv1.ListInstancesRequest) (*registryv1.ListInstancesResponse, error) {
	response, err := s.service.List(ctx, request)
	return response, registryError(err)
}
func (s *registryServer) ListServices(ctx context.Context, request *registryv1.ListServicesRequest) (*registryv1.ListServicesResponse, error) {
	response, err := s.service.ListServices(ctx, request.GetPrefix())
	return response, registryError(err)
}
func (s *registryServer) WatchService(request *registryv1.WatchServiceRequest, stream registryv1.RegistryService_WatchServiceServer) error {
	initial, err := s.service.List(stream.Context(), &registryv1.ListInstancesRequest{ServiceName: request.GetServiceName(), Selector: request.GetSelector(), IncludeDraining: request.GetIncludeDraining()})
	if err != nil {
		return registryError(err)
	}
	changes := make([]*registryv1.InstanceChange, 0, len(initial.Instances))
	for _, instance := range initial.Instances {
		changes = append(changes, &registryv1.InstanceChange{Type: registryv1.InstanceChangeType_INSTANCE_CHANGE_TYPE_SNAPSHOT, Instance: instance, ServiceName: instance.ServiceName, InstanceId: instance.InstanceId})
	}
	if err := stream.Send(&registryv1.WatchServiceResponse{Revision: initial.Revision, Changes: changes}); err != nil {
		return err
	}
	revision := initial.Revision
	for {
		values, readErr := s.store.Changes(stream.Context(), revision, 15*time.Second)
		if readErr != nil {
			return registryError(readErr)
		}
		for _, value := range values {
			revision = value.Revision
			if request.GetServiceName() != "" && value.Service != request.GetServiceName() {
				continue
			}
			change := &registryv1.InstanceChange{ServiceName: value.Service, InstanceId: value.Instance, Instance: value.Value}
			if value.Type == "delete" {
				change.Type = registryv1.InstanceChangeType_INSTANCE_CHANGE_TYPE_DELETE
			} else {
				if value.Value == nil || !registryMatches(value.Value, request.GetSelector(), request.GetIncludeDraining()) {
					change.Type = registryv1.InstanceChangeType_INSTANCE_CHANGE_TYPE_DELETE
					change.Instance = nil
				} else {
					change.Type = registryv1.InstanceChangeType_INSTANCE_CHANGE_TYPE_UPSERT
				}
			}
			if err := stream.Send(&registryv1.WatchServiceResponse{Revision: revision, Changes: []*registryv1.InstanceChange{change}}); err != nil {
				return err
			}
		}
		if err := stream.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
	}
}

func registryMatches(value *registryv1.ServiceInstance, selector *registryv1.MetadataSelector, includeDraining bool) bool {
	if value.Status == registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING && !includeDraining {
		return false
	}
	for key, expected := range selector.GetMatch() {
		if value.Metadata[key] != expected {
			return false
		}
	}
	return true
}

func registryError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, registry.ErrAlreadyRegistered):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, registry.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, registry.ErrLeaseNotOwned):
		return status.Error(codes.PermissionDenied, err.Error())
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}
