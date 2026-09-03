package httptransport

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"github.com/lihongjie0209/service-registry-service/internal/apperror"
	"github.com/lihongjie0209/service-registry-service/internal/buildinfo"
	"github.com/lihongjie0209/service-registry-service/internal/health"
	"github.com/lihongjie0209/service-registry-service/internal/registry"
)

type Handler struct {
	logger   *slog.Logger
	health   *health.Service
	registry *registry.Service
}

func NewHandler(healthService *health.Service, registryService *registry.Service, logger *slog.Logger) *Handler {
	return &Handler{health: healthService, registry: registryService, logger: logger}
}

type MeResponseBody struct {
	Subject string `json:"subject"`
}

// Login godoc
// @Summary Issue a JWT access token
// @Tags authentication
// @Accept json
// @Produce json
// @Param request body LoginRequest true "Client credentials"
// @Success 200 {object} Response{body=LoginResponseBody}
// @Failure 400 {object} Response "Code 10001: invalid request"
// @Failure 401 {object} Response "Code 20001: invalid credentials"
// @Failure 429 {object} Response "Code 10029: rate limited"

// Live godoc
// @Summary Check process liveness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Router /live [post]
func (h *Handler) Live(c *gin.Context) { OK(c, h.health.Live()) }

// Ready godoc
// @Summary Check database and Redis readiness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Failure 503 {object} Response{body=health.Status} "Code 50003: dependency unavailable"
// @Router /ready [post]
func (h *Handler) Ready(c *gin.Context) {
	status, ready := h.health.Ready(c.Request.Context())
	if !ready {
		c.JSON(503, Response{Code: apperror.CodeDependencyUnavailable, Message: "service is not ready", Body: status, RequestID: requestID(c)})
		return
	}
	OK(c, status)
}

// Me godoc
// @Summary Return the authenticated subject
// @Tags authentication
// @Produce json
// @Security Bearer
// @Success 200 {object} Response{body=MeResponseBody}
// @Failure 401 {object} Response "Code 20001: unauthorized"
// @Router /api/v1/me [post]
func (h *Handler) Me(c *gin.Context) {
	subject, _ := c.Get("subject")
	OK(c, gin.H{"subject": subject})
}

// Version godoc
// @Summary Return build and runtime version information
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=buildinfo.Info}
// @Router /api/v1/version [post]
func (h *Handler) Version(c *gin.Context) { OK(c, buildinfo.Current()) }

type ListInstancesRequest struct {
	ServiceName     string            `json:"service_name"`
	Metadata        map[string]string `json:"metadata"`
	IncludeDraining bool              `json:"include_draining"`
	Page            int               `json:"page" binding:"omitempty,min=1"`
	PageSize        int               `json:"page_size" binding:"omitempty,min=1,max=100"`
}
type ListServicesRequest struct {
	Prefix   string `json:"prefix"`
	Page     int    `json:"page" binding:"omitempty,min=1"`
	PageSize int    `json:"page_size" binding:"omitempty,min=1,max=100"`
}
type InstanceBody struct {
	InstanceID     string            `json:"instance_id"`
	ServiceName    string            `json:"service_name"`
	Endpoint       string            `json:"endpoint"`
	Protocol       string            `json:"protocol"`
	Status         string            `json:"status"`
	Weight         uint32            `json:"weight"`
	Version        string            `json:"version"`
	Metadata       map[string]string `json:"metadata"`
	LeaseExpiresAt string            `json:"lease_expires_at"`
}
type ListInstancesBody struct {
	Instances []InstanceBody `json:"instances"`
	Revision  uint64         `json:"revision"`
	Total     int            `json:"total"`
	Page      int            `json:"page"`
	PageSize  int            `json:"page_size"`
}
type ServiceSummaryBody struct {
	ServiceName       string `json:"service_name"`
	HealthyInstances  uint32 `json:"healthy_instances"`
	DrainingInstances uint32 `json:"draining_instances"`
}
type ListServicesBody struct {
	Services []ServiceSummaryBody `json:"services"`
	Revision uint64               `json:"revision"`
	Total    int                  `json:"total"`
	Page     int                  `json:"page"`
	PageSize int                  `json:"page_size"`
}

// ListInstances godoc
// @Summary List live service instances
// @Tags registry
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body ListInstancesRequest true "Service name or metadata selector"
// @Success 200 {object} Response{body=ListInstancesBody}
// @Failure 400 {object} Response "Code 10001: invalid selector"
// @Router /api/v1/registry/instances/list [post]
func (h *Handler) ListInstances(c *gin.Context) {
	var request ListInstancesRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid request", err))
		return
	}
	page, pageSize := pagination(request.Page, request.PageSize)
	response, total, err := h.registry.ListPage(c.Request.Context(), &registryv1.ListInstancesRequest{ServiceName: request.ServiceName, Selector: &registryv1.MetadataSelector{Match: request.Metadata}, IncludeDraining: request.IncludeDraining}, page, pageSize)
	if err != nil {
		Fail(c, h.logger, apperror.Invalid(err.Error(), err))
		return
	}
	body := ListInstancesBody{Revision: response.Revision, Instances: make([]InstanceBody, 0, len(response.Instances)), Total: total, Page: page, PageSize: pageSize}
	for _, value := range response.Instances {
		body.Instances = append(body.Instances, InstanceBody{InstanceID: value.InstanceId, ServiceName: value.ServiceName, Endpoint: value.Endpoint, Protocol: value.Protocol, Status: value.Status.String(), Weight: value.Weight, Version: value.Version, Metadata: value.Metadata, LeaseExpiresAt: value.GetLeaseExpiresAt().AsTime().Format(time.RFC3339)})
	}
	OK(c, body)
}

// ListServices godoc
// @Summary List registered services
// @Tags registry
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body ListServicesRequest true "Optional service prefix"
// @Success 200 {object} Response{body=ListServicesBody}
// @Router /api/v1/registry/services/list [post]
func (h *Handler) ListServices(c *gin.Context) {
	var request ListServicesRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid request", err))
		return
	}
	page, pageSize := pagination(request.Page, request.PageSize)
	response, total, err := h.registry.ListServicesPage(c.Request.Context(), request.Prefix, page, pageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	body := ListServicesBody{Revision: response.Revision, Services: make([]ServiceSummaryBody, 0, len(response.Services)), Total: total, Page: page, PageSize: pageSize}
	for _, value := range response.Services {
		body.Services = append(body.Services, ServiceSummaryBody{ServiceName: value.ServiceName, HealthyInstances: value.HealthyInstances, DrainingInstances: value.DrainingInstances})
	}
	OK(c, body)
}

func pagination(page, pageSize int) (int, int) {
	if page == 0 {
		page = 1
	}
	if pageSize == 0 {
		pageSize = 20
	}
	return page, pageSize
}
