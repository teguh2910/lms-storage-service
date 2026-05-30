package storages

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"time"

	"lms-storage-service/internal/pkg/app"
	"lms-storage-service/internal/pkg/db/redis"
	storagesPb "lms-storage-service/pb/storages"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StorageServiceServer struct
type StorageServiceServer struct {
	Db       *sql.DB
	Cache    *redis.Cache
	Log      *log.Logger
	S3Client *S3Client
	storagesPb.UnimplementedStorageServiceServer
}

// Upload handles streaming file upload to S3
func (s *StorageServiceServer) Upload(stream grpc.ClientStreamingServer[storagesPb.UploadRequest, storagesPb.UploadResponse]) error {
	var fileName string
	var fileType string
	var fileData []byte

	// Get user_id from stream context
	ctx := stream.Context()
	userID := ctx.Value(app.Ctx("user_id")).(string)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.Log.Printf("error receiving upload stream: %v", err)
			return status.Error(codes.Internal, "failed to receive upload data")
		}

		if fileName == "" {
			fileName = req.GetFileName()
			fileType = req.GetFileType()
		}

		fileData = append(fileData, req.GetChunkData()...)
	}

	if fileName == "" {
		return status.Error(codes.InvalidArgument, "file name is required")
	}

	// Generate unique path
	path := fmt.Sprintf("uploads/%s/%d_%s", time.Now().UTC().Format("2006/01/02"), time.Now().UnixNano(), fileName)

	// Upload to S3
	contentType := getContentType(fileType)
	err := s.S3Client.Upload(ctx, path, fileData, contentType)
	if err != nil {
		s.Log.Printf("error uploading to S3: %v", err)
		return status.Error(codes.Internal, "failed to upload file to storage")
	}

	// Save metadata to database
	repo := StorageRepository{Db: s.Db}
	storage := &storagesPb.Storage{
		FileName:  fileName,
		FileType:  fileType,
		FileSize:  int64(len(fileData)),
		Bucket:    s.S3Client.GetBucket(),
		Path:      path,
		UpdatedBy: userID,
	}

	err = repo.Create(ctx, storage)
	if err != nil {
		s.Log.Printf("error saving storage metadata: %v", err)
		return status.Error(codes.Internal, "failed to save file metadata")
	}

	// Generate presigned URL for response
	url, err := s.S3Client.GetPresignedURL(ctx, path)
	if err != nil {
		s.Log.Printf("error generating presigned URL: %v", err)
		// Still return success since file is uploaded
		url = ""
	}

	return stream.SendAndClose(&storagesPb.UploadResponse{
		Id:       storage.Id,
		FileName: storage.FileName,
		Url:      url,
	})
}

// GetFile returns a signed URL for the file
func (s *StorageServiceServer) GetFile(ctx context.Context, in *storagesPb.GetFileRequest) (*storagesPb.GetFileResponse, error) {
	repo := StorageRepository{Db: s.Db}

	storage, err := repo.Get(ctx, in.GetId())
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Error(codes.NotFound, "file not found")
		}
		s.Log.Printf("error getting storage: %v", err)
		return nil, status.Error(codes.Internal, "failed to get file")
	}

	// Generate presigned URL
	signedURL, err := s.S3Client.GetPresignedURL(ctx, storage.Path)
	if err != nil {
		s.Log.Printf("error generating presigned URL: %v", err)
		return nil, status.Error(codes.Internal, "failed to generate signed URL")
	}

	return &storagesPb.GetFileResponse{
		Id:        storage.Id,
		FileName:  storage.FileName,
		SignedUrl:  signedURL,
		FileType:  storage.FileType,
		FileSize:  storage.FileSize,
	}, nil
}

// Delete soft deletes the file record and removes from S3
func (s *StorageServiceServer) Delete(ctx context.Context, in *storagesPb.DeleteRequest) (*storagesPb.DeleteResponse, error) {
	repo := StorageRepository{Db: s.Db}

	// Get storage record first
	storage, err := repo.Get(ctx, in.GetId())
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Error(codes.NotFound, "file not found")
		}
		s.Log.Printf("error getting storage for delete: %v", err)
		return nil, status.Error(codes.Internal, "failed to get file")
	}

	// Soft delete in database
	err = repo.SoftDelete(ctx, in.GetId())
	if err != nil {
		s.Log.Printf("error soft deleting storage: %v", err)
		return nil, status.Error(codes.Internal, "failed to delete file")
	}

	// Delete physical file from S3
	err = s.S3Client.Delete(ctx, storage.Path)
	if err != nil {
		s.Log.Printf("error deleting file from S3: %v", err)
		// File is soft-deleted in DB, log the S3 error but still return success
	}

	return &storagesPb.DeleteResponse{
		Success: true,
	}, nil
}

// ConfirmIsUsed marks a file as being used by another service
func (s *StorageServiceServer) ConfirmIsUsed(ctx context.Context, id string) error {
	repo := StorageRepository{Db: s.Db}
	return repo.ConfirmIsUsed(ctx, id)
}

// DeleteUnusedFiles background job to clean up unused files
func (s *StorageServiceServer) DeleteUnusedFiles(ctx context.Context) error {
	repo := StorageRepository{Db: s.Db}

	unusedFiles, err := repo.GetUnusedFiles(ctx)
	if err != nil {
		s.Log.Printf("error getting unused files: %v", err)
		return err
	}

	for _, file := range unusedFiles {
		// Delete from S3
		err := s.S3Client.Delete(ctx, file.Path)
		if err != nil {
			s.Log.Printf("error deleting unused file from S3 (id: %s): %v", file.Id, err)
			continue
		}

		// Soft delete in DB
		err = repo.SoftDelete(ctx, file.Id)
		if err != nil {
			s.Log.Printf("error soft deleting unused file (id: %s): %v", file.Id, err)
		}
	}

	return nil
}

// getContentType maps file type to MIME content type
func getContentType(fileType string) string {
	switch fileType {
	case "pdf":
		return "application/pdf"
	case "png":
		return "image/png"
	case "jpg", "jpeg":
		return "image/jpeg"
	case "gif":
		return "image/gif"
	case "mp4":
		return "video/mp4"
	case "mp3":
		return "audio/mpeg"
	case "doc":
		return "application/msword"
	case "docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case "xls":
		return "application/vnd.ms-excel"
	case "xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "ppt":
		return "application/vnd.ms-powerpoint"
	case "pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case "zip":
		return "application/zip"
	case "txt":
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}
