package storages

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client wraps the AWS S3 client
type S3Client struct {
	client         *s3.Client
	presignClient  *s3.PresignClient
	bucket         string
	presignExpiry  time.Duration
}

// NewS3Client creates a new S3 client from environment variables
func NewS3Client() (*S3Client, error) {
	region := os.Getenv("AWS_REGION")
	bucket := os.Getenv("AWS_BUCKET")
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if region == "" || bucket == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("missing required AWS environment variables")
	}

	// Presign expiry default 15 minutes
	presignExpiry := 15 * time.Minute
	if v := os.Getenv("AWS_PRESIGN_EXPIRY_MINUTES"); v != "" {
		if minutes, err := strconv.Atoi(v); err == nil {
			presignExpiry = time.Duration(minutes) * time.Minute
		}
	}

	cfg := aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}

	client := s3.NewFromConfig(cfg)
	presignClient := s3.NewPresignClient(client)

	return &S3Client{
		client:        client,
		presignClient: presignClient,
		bucket:        bucket,
		presignExpiry: presignExpiry,
	}, nil
}

// Upload uploads file data to S3
func (s *S3Client) Upload(ctx context.Context, key string, data []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// GetPresignedURL generates a presigned URL for downloading a file
func (s *S3Client) GetPresignedURL(ctx context.Context, key string) (string, error) {
	result, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(s.presignExpiry))
	if err != nil {
		return "", err
	}
	return result.URL, nil
}

// Delete removes a file from S3
func (s *S3Client) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

// GetBucket returns the configured bucket name
func (s *S3Client) GetBucket() string {
	return s.bucket
}
