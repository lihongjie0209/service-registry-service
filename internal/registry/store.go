package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrAlreadyRegistered = errors.New("instance is already registered")
	ErrNotFound          = errors.New("instance was not found")
	ErrLeaseNotOwned     = errors.New("lease is not owned")
)

const keyPrefix = "platform:registry:"

type Store struct {
	redis *redis.Client
	now   func() time.Time
}

func NewStore(client *redis.Client) *Store { return &Store{redis: client, now: time.Now} }

func instanceKey(service, instance string) string {
	return keyPrefix + "instance:" + service + ":" + instance
}
func tokenKey(service, instance string) string {
	return keyPrefix + "token:" + service + ":" + instance
}
func serviceKey(service string) string { return keyPrefix + "service:" + service }
func revisionKey() string              { return keyPrefix + "revision" }
func streamKey() string                { return keyPrefix + "changes" }
func servicesKey() string              { return keyPrefix + "services" }

var registerScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return {err='ALREADY_REGISTERED'} end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
redis.call('SADD', KEYS[3], ARGV[4])
redis.call('SADD', KEYS[6], ARGV[5])
local revision = redis.call('INCR', KEYS[4])
redis.call('XADD', KEYS[5], revision .. '-0', 'revision', revision, 'type', 'upsert', 'service', ARGV[5], 'instance', ARGV[4], 'payload', ARGV[1])
return revision
`)

var updateScript = redis.NewScript(`
local token = redis.call('GET', KEYS[2])
if not token then return {err='NOT_FOUND'} end
if token ~= ARGV[2] then return {err='LEASE_NOT_OWNED'} end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
redis.call('PEXPIRE', KEYS[2], ARGV[3])
local revision = redis.call('INCR', KEYS[3])
redis.call('XADD', KEYS[4], revision .. '-0', 'revision', revision, 'type', 'upsert', 'service', ARGV[4], 'instance', ARGV[5], 'payload', ARGV[1])
return revision
`)

var deleteScript = redis.NewScript(`
local token = redis.call('GET', KEYS[2])
if not token then return {err='NOT_FOUND'} end
if token ~= ARGV[1] then return {err='LEASE_NOT_OWNED'} end
redis.call('DEL', KEYS[1], KEYS[2])
redis.call('SREM', KEYS[3], ARGV[2])
local revision = redis.call('INCR', KEYS[4])
redis.call('XADD', KEYS[5], revision .. '-0', 'revision', revision, 'type', 'delete', 'service', ARGV[3], 'instance', ARGV[2])
return revision
`)

func (s *Store) Register(ctx context.Context, instance *registryv1.ServiceInstance, token string, lease time.Duration) (uint64, error) {
	payload, err := json.Marshal(instance)
	if err != nil {
		return 0, fmt.Errorf("marshal instance: %w", err)
	}
	result, err := registerScript.Run(ctx, s.redis, []string{instanceKey(instance.ServiceName, instance.InstanceId), tokenKey(instance.ServiceName, instance.InstanceId), serviceKey(instance.ServiceName), revisionKey(), streamKey(), servicesKey()}, string(payload), hashToken(token), lease.Milliseconds(), instance.InstanceId, instance.ServiceName).Uint64()
	return result, mapScriptError(err)
}

func (s *Store) Update(ctx context.Context, instance *registryv1.ServiceInstance, token string, lease time.Duration) (uint64, error) {
	payload, err := json.Marshal(instance)
	if err != nil {
		return 0, fmt.Errorf("marshal instance: %w", err)
	}
	result, err := updateScript.Run(ctx, s.redis, []string{instanceKey(instance.ServiceName, instance.InstanceId), tokenKey(instance.ServiceName, instance.InstanceId), revisionKey(), streamKey()}, string(payload), hashToken(token), lease.Milliseconds(), instance.ServiceName, instance.InstanceId).Uint64()
	return result, mapScriptError(err)
}

func (s *Store) Deregister(ctx context.Context, service, instance, token string) (uint64, error) {
	result, err := deleteScript.Run(ctx, s.redis, []string{instanceKey(service, instance), tokenKey(service, instance), serviceKey(service), revisionKey(), streamKey()}, hashToken(token), instance, service).Uint64()
	return result, mapScriptError(err)
}

func (s *Store) Get(ctx context.Context, service, instance string) (*registryv1.ServiceInstance, error) {
	payload, err := s.redis.Get(ctx, instanceKey(service, instance)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get instance: %w", err)
	}
	var value registryv1.ServiceInstance
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("decode instance: %w", err)
	}
	return &value, nil
}

func (s *Store) List(ctx context.Context, service string) ([]*registryv1.ServiceInstance, uint64, error) {
	ids, err := s.redis.SMembers(ctx, serviceKey(service)).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("list instance ids: %w", err)
	}
	instances := make([]*registryv1.ServiceInstance, 0, len(ids))
	stale := make([]any, 0)
	for _, id := range ids {
		value, getErr := s.Get(ctx, service, id)
		if errors.Is(getErr, ErrNotFound) {
			stale = append(stale, id)
			continue
		}
		if getErr != nil {
			return nil, 0, getErr
		}
		instances = append(instances, value)
	}
	if len(stale) > 0 {
		_ = s.redis.SRem(ctx, serviceKey(service), stale...).Err()
	}
	revision, err := s.redis.Get(ctx, revisionKey()).Uint64()
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	return instances, revision, err
}

func (s *Store) Services(ctx context.Context) ([]string, uint64, error) {
	services, err := s.redis.SMembers(ctx, servicesKey()).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("list services: %w", err)
	}
	revision, err := s.redis.Get(ctx, revisionKey()).Uint64()
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	return services, revision, err
}

type Change struct {
	Revision                uint64
	Type, Service, Instance string
	Value                   *registryv1.ServiceInstance
}

func (s *Store) Changes(ctx context.Context, after uint64, block time.Duration) ([]Change, error) {
	start := strconv.FormatUint(after, 10) + "-0"
	streams, err := s.redis.XRead(ctx, &redis.XReadArgs{Streams: []string{streamKey(), start}, Block: block, Count: 128}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read registry changes: %w", err)
	}
	changes := make([]Change, 0)
	for _, stream := range streams {
		for _, message := range stream.Messages {
			revision, parseErr := strconv.ParseUint(strings.Split(message.ID, "-")[0], 10, 64)
			if parseErr != nil {
				continue
			}
			change := Change{Revision: revision, Type: fmt.Sprint(message.Values["type"]), Service: fmt.Sprint(message.Values["service"]), Instance: fmt.Sprint(message.Values["instance"])}
			if payload := fmt.Sprint(message.Values["payload"]); payload != "<nil>" && payload != "" {
				var value registryv1.ServiceInstance
				if json.Unmarshal([]byte(payload), &value) == nil {
					change.Value = &value
				}
			}
			changes = append(changes, change)
		}
	}
	return changes, nil
}

func prepareInstance(input *registryv1.ServiceInstance, lease time.Duration, now time.Time) *registryv1.ServiceInstance {
	copy := proto.Clone(input).(*registryv1.ServiceInstance)
	copy.Metadata = cloneMap(input.GetMetadata())
	copy.RegisteredAt = timestamppb.New(now)
	copy.UpdatedAt = timestamppb.New(now)
	copy.LeaseExpiresAt = timestamppb.New(now.Add(lease))
	if copy.Weight == 0 {
		copy.Weight = 100
	}
	if copy.Status == registryv1.InstanceStatus_INSTANCE_STATUS_UNSPECIFIED {
		copy.Status = registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY
	}
	return copy
}

func cloneMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func mapScriptError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case strings.Contains(err.Error(), "ALREADY_REGISTERED"):
		return ErrAlreadyRegistered
	case strings.Contains(err.Error(), "NOT_FOUND"):
		return ErrNotFound
	case strings.Contains(err.Error(), "LEASE_NOT_OWNED"):
		return ErrLeaseNotOwned
	default:
		return err
	}
}
