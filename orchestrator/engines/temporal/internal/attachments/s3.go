package attachments

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config is the attachment store's connection profile. It uses the same
// key names as the image-gen tool's S3 secret (IMAGE_S3_*), so a deployment
// can point both at one bucket profile.
type S3Config struct {
	Bucket          string
	Region          string
	Endpoint        string // empty = AWS's default endpoint
	ForcePathStyle  bool   // MinIO / Ceph RGW
	Prefix          string // key prefix, default "attachments"
	AccessKeyID     string // empty = the default credential chain (IRSA, ...)
	SecretAccessKey string
}

// S3ConfigFromEnv reads the profile from the environment. ok=false when no
// bucket is set: attachments are then disabled.
func S3ConfigFromEnv() (S3Config, bool) {
	cfg := S3Config{
		Bucket:          strings.TrimSpace(os.Getenv("IMAGE_S3_BUCKET")),
		Region:          strings.TrimSpace(os.Getenv("IMAGE_S3_REGION")),
		Endpoint:        strings.TrimSpace(os.Getenv("IMAGE_S3_ENDPOINT")),
		ForcePathStyle:  strings.EqualFold(strings.TrimSpace(os.Getenv("IMAGE_S3_FORCE_PATH_STYLE")), "true"),
		Prefix:          strings.Trim(strings.TrimSpace(os.Getenv("ATTACHMENTS_S3_PREFIX")), "/"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "attachments"
	}
	return cfg, cfg.Bucket != ""
}

// S3Store is a Store on an S3-compatible bucket.
type S3Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
}

// NewS3Store builds a store from cfg. With no static keys it uses the AWS
// default credential chain, so an EKS deployment can use IRSA.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	return &S3Store{client: client, presign: s3.NewPresignClient(client), bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

// KeyPrefix is where this store's objects live; callers append their own
// scoping below it.
func (s *S3Store) KeyPrefix() string { return s.prefix }

func (s *S3Store) Put(ctx context.Context, key, mediaType string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentType:   aws.String(mediaType),
		ContentLength: aws.Int64(int64(len(data))),
	})
	return err
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(io.LimitReader(out.Body, MaxImageBytes+1))
}

func (s *S3Store) Presign(ctx context.Context, key string, ttl time.Duration) (string, error) {
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)},
		s3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}
