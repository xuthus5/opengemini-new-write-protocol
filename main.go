package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	conn, err := grpc.Dial(":8080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	mock := NewTransform()
	mock.AppendLine(map[string]string{"a": "1"}, map[string]interface{}{"b": 1}, int64(time.Now().Nanosecond()))
	time.Sleep(time.Second)
	mock.AppendLine(map[string]string{"a": "1"}, map[string]interface{}{"b": 2}, int64(time.Now().Nanosecond()))

	record := mock.ToSrvRecords()

	var buff []byte
	buff, err = record.Marshal(buff)
	if err != nil {
		panic(err)
	}

	client := NewWriteServiceClient(conn)
	response, err := client.WriteRows(ctx, &WriteRowsRequest{
		Version:  0,
		Database: "",
		Rp:       "",
		Username: "",
		Password: "",
		Rows: &Rows{
			MinTime:      mock.MinTime,
			MaxTime:      mock.MaxTime,
			CompressAlgo: 0,
			Block:        buff,
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("response: %+v\n", response)
}
