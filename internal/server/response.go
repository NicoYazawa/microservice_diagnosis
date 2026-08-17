// Package server provides HTTP server setup with Gin + gRPC-gateway.
package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorResponse represents a JSON error payload.
type errorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Details string `json:"details,omitempty"`
}

func jsonError(c *gin.Context, httpStatus int, errMsg string, grpcCode codes.Code) {
	c.JSON(httpStatus, errorResponse{
		Error: errMsg,
		Code:  grpcCode.String(),
	})
}

func notFound(c *gin.Context, msg string) {
	jsonError(c, http.StatusNotFound, msg, codes.NotFound)
}

func badRequest(c *gin.Context, msg string) {
	jsonError(c, http.StatusBadRequest, msg, codes.InvalidArgument)
}

func internalError(c *gin.Context, msg string) {
	jsonError(c, http.StatusInternalServerError, msg, codes.Internal)
}

func toGrpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%v", err)
}
