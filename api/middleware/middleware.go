package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
)

func Logger(log logger.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method
		clientIP := c.ClientIP()

		requestID := uuid.New().String()
		c.Set("logger", log.WithField("requestID", requestID).WithField("path", path).WithField("method", method).WithField("clientIP", clientIP))
		c.Next()

		latency := time.Since(start)
		statusCode := c.Writer.Status()
		size := c.Writer.Size()

		log.
			WithField("requestID", requestID).
			WithField("path", path).
			WithField("method", method).
			WithField("clientIP", clientIP).
			WithField("status", statusCode).
			WithField("latency", latency.String()).
			WithField("size", size).
			Info("request")
	}
}
