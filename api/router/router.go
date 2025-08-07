package router

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/handlers"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/middleware"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
)

// Setup 设置路由
func Setup(log logger.Logger, handler *handlers.CatalogHandler) *gin.Engine {
	// 创建处理器
	engine := gin.New()
	engine.Use(middleware.Logger(log))
	engine.Use(cors.Default())
	engine.Use(gin.Recovery())

	// API v1 路由组
	v1 := engine.Group("/v1")
	{
		// 配置 API
		v1.GET("/config", handler.GetConfig)

		// 带前缀的路由组
		prefixGroup := v1.Group("/:prefix")
		{
			// 命名空间 API
			namespaces := prefixGroup.Group("/namespaces")
			{
				namespaces.GET("", handler.ListNamespaces)
				namespaces.POST("", handler.CreateNamespace)

				namespace := namespaces.Group("/:namespace")
				{
					namespace.GET("", handler.LoadNamespaceMetadata)
					namespace.HEAD("", handler.NamespaceExists)
					namespace.DELETE("", handler.DropNamespace)
					namespace.POST("/properties", handler.UpdateProperties)

					// 表 API
					tables := namespace.Group("/tables")
					{
						tables.GET("", handler.ListTables)
						tables.POST("", handler.CreateTable)

						table := tables.Group("/:table")
						{
							table.GET("", handler.LoadTable)
							table.POST("", handler.UpdateTable)
							table.DELETE("", handler.DropTable)
							table.HEAD("", handler.TableExists)
						}
					}
				}
			}

			// 表重命名 API
			prefixGroup.POST("/tables/rename", handler.RenameTable)
		}
	}

	// 健康检查
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	return engine
}
