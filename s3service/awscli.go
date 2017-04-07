package s3service

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
)

type AWSResponse struct {
	Reader    io.ReadCloser
	ByteCount int64
}

func (r *AWSResponse) Close() {
	r.Reader.Close()
}

func GetFileReader(bucket string, file string) *AWSResponse {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1"),
	})

	fmt.Printf("Requesting s3://%s/%s\n", bucket, file)

	service := s3.New(sess)
	request := &s3.GetObjectInput{}
	request.SetBucket(bucket)
	request.SetKey(file)

	result, err := service.GetObject(request)
	if err != nil {
		log.Fatal(err.Error())
	}
	return &AWSResponse{Reader: result.Body, ByteCount: *result.ContentLength}
}
