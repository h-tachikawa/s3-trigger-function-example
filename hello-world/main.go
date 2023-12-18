package main

import (
	"bytes"
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"os"
)

func handler(ctx context.Context, s3Event events.S3Event) error {
	endpoint := os.Getenv("S3_ENDPOINT")

	if _, err := os.Stat("/tmp"); os.IsNotExist(err) {
		if err := os.Mkdir("/tmp", os.ModePerm); err != nil {
			log.Printf("error when creating tmp directory. %v", err)
			return err
		}
	}

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID && len(endpoint) > 0 {
			return aws.Endpoint{
				URL:               endpoint,
				HostnameImmutable: true,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	sdkConfig, err := config.LoadDefaultConfig(
		ctx,
		config.WithEndpointResolverWithOptions(resolver),
	)

	if err != nil {
		log.Printf("failed to load default config: %s", err)
		return err
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	record := s3Event.Records[0]
	bucket := record.S3.Bucket.Name
	key := record.S3.Object.Key
	image, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	if err != nil {
		return err
	}

	targetFilePath := "/tmp/test-copied.jpeg"
	file, err := os.Create(targetFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(image.Body)
	_, err = file.Write(buf.Bytes())
	newKey := "/test-copied.jpeg"

	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &newKey,
		Body:   file,
	})

	if err != nil {
		return err
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
