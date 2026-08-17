package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var (
	ErrNotFound = errors.New("not found")
	ErrNotEmpty = errors.New("not empty")
)

type Config struct {
	Endpoint       string
	AccessKey      string
	SecretKey      string
	Region         string
	UploadTTL      time.Duration
	DownloadTTL    time.Duration
	DownloadCDNURL string
	PreviewCDNURL  string
}

func (c Config) Ready() bool {
	return c.Endpoint != "" && c.AccessKey != "" && c.SecretKey != ""
}

type Client struct {
	s3             *s3.Client
	presign        *s3.PresignClient
	uploadTTL      time.Duration
	downloadTTL    time.Duration
	downloadCDNURL string
	previewCDNURL  string
}

type ObjectMeta struct {
	Size        int64
	ETag        *string
	ContentType *string
}

type CompletedPart struct {
	PartNumber int32
	ETag       string
}

func New(cfg Config) *Client {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	awscfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
	}
	cli := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(strings.TrimRight(cfg.Endpoint, "/"))
		o.UsePathStyle = true // ponytail: RGW expects path-style; virtual-host if a vendor requires it
	})
	up, down := cfg.UploadTTL, cfg.DownloadTTL
	if up <= 0 {
		up = 30 * time.Minute
	}
	if down <= 0 {
		down = 10 * time.Minute
	}
	return &Client{
		s3: cli, presign: s3.NewPresignClient(cli), uploadTTL: up, downloadTTL: down,
		downloadCDNURL: cfg.DownloadCDNURL, previewCDNURL: cfg.PreviewCDNURL,
	}
}

func (c *Client) SetCDN(download, preview string) {
	c.downloadCDNURL = download
	c.previewCDNURL = preview
}

func (c *Client) EnsureBucket(ctx context.Context, name string) error {
	_, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)})
	if err == nil {
		return nil
	}
	var exists *types.BucketAlreadyExists
	var owned *types.BucketAlreadyOwnedByYou
	if errors.As(err, &exists) || errors.As(err, &owned) {
		return nil
	}
	return err
}

func (c *Client) DeleteBucket(ctx context.Context, name string) error {
	_, err := c.s3.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	if err == nil {
		return nil
	}
	if isNotFound(err) {
		return ErrNotFound
	}
	if apiCode(err) == "BucketNotEmpty" {
		return ErrNotEmpty
	}
	return err
}

func (c *Client) HeadObject(ctx context.Context, bucket, key string) (ObjectMeta, error) {
	out, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return ObjectMeta{}, ErrNotFound
		}
		return ObjectMeta{}, err
	}
	meta := ObjectMeta{ContentType: out.ContentType, ETag: stripETag(out.ETag)}
	if out.ContentLength != nil {
		meta.Size = *out.ContentLength
	}
	return meta, nil
}

func (c *Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, contentType string) (*string, error) {
	in := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: body}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	out, err := c.s3.PutObject(ctx, in)
	if err != nil {
		return nil, err
	}
	return stripETag(out.ETag), nil
}

func (c *Client) GetObject(ctx context.Context, bucket, key string, maxBytes int64) ([]byte, *string, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	defer out.Body.Close()
	var r io.Reader = out.Body
	if maxBytes > 0 {
		r = io.LimitReader(out.Body, maxBytes)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}
	return body, out.ContentType, nil
}

func (c *Client) PresignPut(ctx context.Context, bucket, key, contentType string) (string, int, error) {
	in := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	out, err := c.presign.PresignPutObject(ctx, in, s3.WithPresignExpires(c.uploadTTL))
	if err != nil {
		return "", 0, err
	}
	return out.URL, int(c.uploadTTL.Seconds()), nil
}

func (c *Client) DownloadTTL() time.Duration { return c.downloadTTL }

func (c *Client) PresignGet(ctx context.Context, bucket, key string) (string, int, error) {
	return c.PresignGetFor(ctx, bucket, key, c.downloadTTL)
}

func (c *Client) PresignGetFor(ctx context.Context, bucket, key string, ttl time.Duration) (string, int, error) {
	if ttl < time.Second {
		return "", 0, errors.New("expired")
	}
	out, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", 0, err
	}
	return rewriteCDN(out.URL, c.downloadCDNURL), int(ttl.Seconds()), nil
}

func (c *Client) PresignUploadPart(ctx context.Context, bucket, key, uploadID string, part int32) (string, error) {
	out, err := c.presign.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		UploadId: aws.String(uploadID), PartNumber: aws.Int32(part),
	}, s3.WithPresignExpires(c.uploadTTL))
	if err != nil {
		return "", err
	}
	return out.URL, nil
}

func (c *Client) CreateMultipart(ctx context.Context, bucket, key, contentType string) (string, error) {
	in := &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	out, err := c.s3.CreateMultipartUpload(ctx, in)
	if err != nil {
		return "", err
	}
	if out.UploadId == nil || *out.UploadId == "" {
		return "", errors.New("CreateMultipartUpload did not return UploadId")
	}
	return *out.UploadId, nil
}

func (c *Client) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []CompletedPart) error {
	completed := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		etag := p.ETag
		n := p.PartNumber
		completed[i] = types.CompletedPart{ETag: &etag, PartNumber: &n}
	}
	_, err := c.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	return err
}

func (c *Client) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	_, err := c.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
	return err
}

func (c *Client) CopyObject(ctx context.Context, destBucket, destKey, srcBucket, srcKey string) (*string, error) {
	out, err := c.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(destKey),
		CopySource: aws.String(srcBucket + "/" + srcKey),
	})
	if err != nil {
		return nil, err
	}
	if out.CopyObjectResult == nil {
		return nil, nil
	}
	return stripETag(out.CopyObjectResult.ETag), nil
}

func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func (c *Client) PutBucketVersioning(ctx context.Context, bucket, status string) error {
	st := types.BucketVersioningStatusSuspended
	if status == "Enabled" {
		st = types.BucketVersioningStatusEnabled
	}
	_, err := c.s3.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: st},
	})
	return err
}

func (c *Client) GetBucketPolicy(ctx context.Context, bucket string) (map[string]any, error) {
	out, err := c.s3.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		if isNoPolicy(err) {
			return nil, nil
		}
		return nil, err
	}
	if out.Policy == nil || *out.Policy == "" {
		return nil, nil
	}
	var doc any
	if err := json.Unmarshal([]byte(*out.Policy), &doc); err != nil {
		return nil, err
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, errors.New("Bucket policy is not a JSON object")
	}
	return obj, nil
}

func (c *Client) PutBucketPolicy(ctx context.Context, bucket string, policy map[string]any) error {
	raw, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	_, err = c.s3.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(string(raw)),
	})
	return err
}

func (c *Client) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	_, err := c.s3.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil && isNoPolicy(err) {
		return nil
	}
	return err
}

type CORSRule struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
	ExposeHeaders  []string
	MaxAgeSeconds  *int32
}

func (c *Client) GetBucketCORS(ctx context.Context, bucket string) ([]CORSRule, error) {
	out, err := c.s3.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	if err != nil {
		if isNoCORS(err) {
			return nil, nil
		}
		return nil, err
	}
	return corsFromS3(out.CORSRules), nil
}

func (c *Client) PutBucketCORS(ctx context.Context, bucket string, rules []CORSRule) error {
	_, err := c.s3.PutBucketCors(ctx, &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucket),
		CORSConfiguration: &types.CORSConfiguration{CORSRules: corsToS3(rules)},
	})
	return err
}

func (c *Client) DeleteBucketCORS(ctx context.Context, bucket string) error {
	_, err := c.s3.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)})
	if err != nil && isNoCORS(err) {
		return nil
	}
	return err
}

func (c *Client) UploadTTLSeconds() int { return int(c.uploadTTL.Seconds()) }

type ListedObject struct {
	Key          string
	Size         int64
	ETag         *string
	StorageClass *string
}

type ListPage struct {
	Objects   []ListedObject
	Truncated bool
	Token     string
}

func (c *Client) ListPrefixFlat(ctx context.Context, bucket, prefix, token string, maxKeys int32) (ListPage, error) {
	return c.list(ctx, bucket, prefix, token, maxKeys)
}

func (c *Client) ListObjects(ctx context.Context, bucket, token string, maxKeys int32) (ListPage, error) {
	return c.list(ctx, bucket, "", token, maxKeys)
}

func (c *Client) list(ctx context.Context, bucket, prefix, token string, maxKeys int32) (ListPage, error) {
	if maxKeys < 1 {
		maxKeys = 1000
	}
	in := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), MaxKeys: aws.Int32(maxKeys)}
	if prefix != "" {
		in.Prefix = aws.String(prefix)
	}
	if token != "" {
		in.ContinuationToken = aws.String(token)
	}
	out, err := c.s3.ListObjectsV2(ctx, in)
	if err != nil {
		return ListPage{}, err
	}
	page := ListPage{Truncated: aws.ToBool(out.IsTruncated)}
	if out.NextContinuationToken != nil {
		page.Token = *out.NextContinuationToken
	}
	for _, obj := range out.Contents {
		item := ListedObject{Size: aws.ToInt64(obj.Size), ETag: stripETag(obj.ETag), StorageClass: (*string)(nil)}
		if obj.Key != nil {
			item.Key = *obj.Key
		}
		if obj.StorageClass != "" {
			sc := string(obj.StorageClass)
			item.StorageClass = &sc
		}
		page.Objects = append(page.Objects, item)
	}
	return page, nil
}

func corsToS3(rules []CORSRule) []types.CORSRule {
	out := make([]types.CORSRule, 0, len(rules))
	for _, r := range rules {
		item := types.CORSRule{
			AllowedOrigins: r.AllowedOrigins,
			AllowedMethods: r.AllowedMethods,
		}
		if len(r.AllowedHeaders) > 0 {
			item.AllowedHeaders = r.AllowedHeaders
		}
		if len(r.ExposeHeaders) > 0 {
			item.ExposeHeaders = r.ExposeHeaders
		}
		item.MaxAgeSeconds = r.MaxAgeSeconds
		out = append(out, item)
	}
	return out
}

func corsFromS3(rules []types.CORSRule) []CORSRule {
	out := make([]CORSRule, 0, len(rules))
	for _, r := range rules {
		item := CORSRule{
			AllowedOrigins: append([]string(nil), r.AllowedOrigins...),
			AllowedMethods: append([]string(nil), r.AllowedMethods...),
			AllowedHeaders: append([]string(nil), r.AllowedHeaders...),
			ExposeHeaders:  append([]string(nil), r.ExposeHeaders...),
		}
		if r.MaxAgeSeconds != nil {
			n := *r.MaxAgeSeconds
			item.MaxAgeSeconds = &n
		}
		out = append(out, item)
	}
	return out
}

func isNoPolicy(err error) bool {
	return apiCode(err) == "NoSuchBucketPolicy"
}

func isNoCORS(err error) bool {
	code := apiCode(err)
	return code == "NoSuchCORSConfiguration" || code == "NoSuchBucketCORSConfiguration"
}

func apiCode(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return ""
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nsb *types.NoSuchBucket
	var nf *types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nsb) || errors.As(err, &nf) {
		return true
	}
	code := apiCode(err)
	return code == "NotFound" || code == "NoSuchKey" || code == "NoSuchBucket" || code == "404"
}

func stripETag(etag *string) *string {
	if etag == nil {
		return nil
	}
	s := strings.Trim(*etag, `"`)
	return &s
}
