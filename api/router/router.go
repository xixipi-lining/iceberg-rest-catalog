package router

import (
	"github.com/gin-gonic/gin"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/handlers"
)

// Setup 设置路由
func Setup(engine *gin.Engine, handler *handlers.CatalogHandler) *gin.Engine {
	// 创建处理器

	v1 := engine.Group("/v1")
	{
		v1.GET("/config", handler.GetConfig)

		namespaces := v1.Group("/namespaces")
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
		v1.POST("/tables/rename", handler.RenameTable)
	}

	// 健康检查
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	return engine
}
