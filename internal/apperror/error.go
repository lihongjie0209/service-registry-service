package apperror

import "net/http"

type Error struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"-"`
	Err        error  `json:"-"`
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Message
}
func (e *Error) Unwrap() error { return e.Err }

const (
	CodeOK                    = 0
	CodeInvalidArgument       = 10001
	CodeNotFound              = 10004
	CodeRequestTimeout        = 10008
	CodeTooManyRequests       = 10029
	CodeUnauthorized          = 20001
	CodeForbidden             = 20003
	CodeConflict              = 30009
	CodeRequestInProgress     = 30010
	CodeInternal              = 50000
	CodeDependencyUnavailable = 50003
)

func New(code int, message string, status int, err error) *Error {
	return &Error{Code: code, Message: message, HTTPStatus: status, Err: err}
}
func Invalid(message string, err error) *Error {
	return New(CodeInvalidArgument, message, http.StatusBadRequest, err)
}
func NotFound(message string) *Error {
	return New(CodeNotFound, message, http.StatusNotFound, nil)
}
func Conflict(message string, err error) *Error {
	return New(CodeConflict, message, http.StatusConflict, err)
}
func RequestInProgress() *Error {
	return New(CodeRequestInProgress, "request is already processing", http.StatusConflict, nil)
}
func Unauthorized(message string) *Error {
	return New(CodeUnauthorized, message, http.StatusUnauthorized, nil)
}
func Forbidden(message string) *Error {
	return New(CodeForbidden, message, http.StatusForbidden, nil)
}
func TooManyRequests() *Error {
	return New(CodeTooManyRequests, "too many requests", http.StatusTooManyRequests, nil)
}
func RequestTimeout() *Error {
	return New(CodeRequestTimeout, "request timeout", http.StatusGatewayTimeout, nil)
}
func Unavailable(message string, err error) *Error {
	return New(CodeDependencyUnavailable, message, http.StatusServiceUnavailable, err)
}
func Internal(err error) *Error {
	return New(CodeInternal, "internal server error", http.StatusInternalServerError, err)
}
