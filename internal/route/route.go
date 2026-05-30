package route

import (
	"database/sql"
	"log"

	"google.golang.org/grpc"

	storagesDomain "lms-storage-service/internal/domain/storages"
	"lms-storage-service/internal/pkg/db/redis"
	storagesPb "lms-storage-service/pb/storages"
)

// GrpcRoute func
func GrpcRoute(grpcServer *grpc.Server, db *sql.DB, log *log.Logger, cache *redis.Cache, s3Client *storagesDomain.S3Client) {
	storageServer := storagesDomain.StorageServiceServer{
		Db:       db,
		Cache:    cache,
		Log:      log,
		S3Client: s3Client,
	}
	storagesPb.RegisterStorageServiceServer(grpcServer, &storageServer)
}
