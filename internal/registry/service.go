package registry

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultLease = 30 * time.Second

var namePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

type Service struct {
	store *Store
	now   func() time.Time
}

func NewService(store *Store) *Service { return &Service{store: store, now: time.Now} }

func (s *Service) Register(ctx context.Context, request *registryv1.RegisterInstanceRequest) (*registryv1.RegisterInstanceResponse, error) {
	if err := validateInstance(request.GetInstance()); err != nil {
		return nil, err
	}
	lease := leaseDuration(request.GetLeaseSeconds())
	instance := prepareInstance(request.Instance, lease, s.now())
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, err
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	revision, err := s.store.Register(ctx, instance, token, lease)
	if err != nil {
		return nil, err
	}
	return &registryv1.RegisterInstanceResponse{Instance: instance, LeaseToken: token, Revision: revision}, nil
}

func (s *Service) Renew(ctx context.Context, request *registryv1.RenewLeaseRequest) (*registryv1.RenewLeaseResponse, error) {
	instance, err := s.store.Get(ctx, request.GetServiceName(), request.GetInstanceId())
	if err != nil {
		return nil, err
	}
	lease := leaseDuration(request.GetLeaseSeconds())
	now := s.now()
	instance.UpdatedAt = timestamppb.New(now)
	instance.LeaseExpiresAt = timestamppb.New(now.Add(lease))
	revision, err := s.store.Update(ctx, instance, request.GetLeaseToken(), lease)
	if err != nil {
		return nil, err
	}
	return &registryv1.RenewLeaseResponse{Instance: instance, Revision: revision}, nil
}

func (s *Service) Deregister(ctx context.Context, request *registryv1.DeregisterInstanceRequest) (uint64, error) {
	return s.store.Deregister(ctx, request.GetServiceName(), request.GetInstanceId(), request.GetLeaseToken())
}

func (s *Service) SetStatus(ctx context.Context, request *registryv1.SetInstanceStatusRequest) (*registryv1.SetInstanceStatusResponse, error) {
	if request.GetStatus() != registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY && request.GetStatus() != registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING {
		return nil, errors.New("status must be healthy or draining")
	}
	instance, err := s.store.Get(ctx, request.GetServiceName(), request.GetInstanceId())
	if err != nil {
		return nil, err
	}
	remaining := time.Until(instance.GetLeaseExpiresAt().AsTime())
	if remaining <= 0 {
		return nil, ErrNotFound
	}
	instance.Status = request.Status
	instance.UpdatedAt = timestamppb.New(s.now())
	revision, err := s.store.Update(ctx, instance, request.GetLeaseToken(), remaining)
	if err != nil {
		return nil, err
	}
	return &registryv1.SetInstanceStatusResponse{Instance: instance, Revision: revision}, nil
}

func (s *Service) List(ctx context.Context, request *registryv1.ListInstancesRequest) (*registryv1.ListInstancesResponse, error) {
	if request.GetServiceName() == "" && len(request.GetSelector().GetMatch()) == 0 {
		return nil, errors.New("service_name or metadata selector is required")
	}
	if request.GetServiceName() != "" && !namePattern.MatchString(request.GetServiceName()) {
		return nil, errors.New("invalid service name")
	}
	serviceNames := []string{request.GetServiceName()}
	var revision uint64
	if request.GetServiceName() == "" {
		var err error
		serviceNames, revision, err = s.store.Services(ctx)
		if err != nil {
			return nil, err
		}
	}
	values := make([]*registryv1.ServiceInstance, 0)
	for _, serviceName := range serviceNames {
		instances, currentRevision, err := s.store.List(ctx, serviceName)
		if err != nil {
			return nil, err
		}
		values = append(values, instances...)
		if currentRevision > revision {
			revision = currentRevision
		}
	}
	filtered := values[:0]
	for _, value := range values {
		if matches(value, request.GetSelector(), request.GetIncludeDraining()) {
			filtered = append(filtered, value)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].InstanceId < filtered[j].InstanceId })
	return &registryv1.ListInstancesResponse{Instances: filtered, Revision: revision}, nil
}

func (s *Service) Get(ctx context.Context, serviceName, instanceID string) (*registryv1.ServiceInstance, error) {
	if !namePattern.MatchString(serviceName) || instanceID == "" {
		return nil, errors.New("service_name and instance_id are required")
	}
	return s.store.Get(ctx, serviceName, instanceID)
}

func (s *Service) ListServices(ctx context.Context, prefix string) (*registryv1.ListServicesResponse, error) {
	names, revision, err := s.store.Services(ctx)
	if err != nil {
		return nil, err
	}
	result := &registryv1.ListServicesResponse{Revision: revision}
	for _, name := range names {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		instances, _, listErr := s.store.List(ctx, name)
		if listErr != nil {
			return nil, listErr
		}
		summary := &registryv1.ServiceSummary{ServiceName: name}
		for _, instance := range instances {
			switch instance.Status {
			case registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY:
				summary.HealthyInstances++
			case registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING:
				summary.DrainingInstances++
			}
		}
		if summary.HealthyInstances+summary.DrainingInstances > 0 {
			result.Services = append(result.Services, summary)
		}
	}
	sort.Slice(result.Services, func(i, j int) bool { return result.Services[i].ServiceName < result.Services[j].ServiceName })
	return result, nil
}

func matches(value *registryv1.ServiceInstance, selector *registryv1.MetadataSelector, includeDraining bool) bool {
	if value.Status == registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING && !includeDraining {
		return false
	}
	if value.Status != registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY && value.Status != registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING {
		return false
	}
	for key, expected := range selector.GetMatch() {
		if value.Metadata[key] != expected {
			return false
		}
	}
	return true
}

func validateInstance(instance *registryv1.ServiceInstance) error {
	if instance == nil || !namePattern.MatchString(instance.GetServiceName()) || instance.GetInstanceId() == "" {
		return errors.New("service_name and instance_id are required")
	}
	parsed, err := url.Parse(instance.GetEndpoint())
	if err != nil || parsed.Host == "" || (parsed.Scheme != "grpc" && parsed.Scheme != "grpcs" && parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("endpoint must use grpc, grpcs, http, or https")
	}
	return nil
}
func leaseDuration(seconds uint32) time.Duration {
	if seconds == 0 {
		return defaultLease
	}
	if seconds < 5 {
		seconds = 5
	}
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}
