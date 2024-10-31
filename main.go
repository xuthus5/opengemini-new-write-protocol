package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	conn, err := grpc.NewClient("127.0.0.1:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	mock := NewTransform("db0", "rp0", "mst0")
	if err := mock.AppendLine(map[string]string{"a": "1"}, map[string]interface{}{"b": 1}, time.Now().UnixNano()); err != nil {
		panic(err)
	}
	if err := mock.AppendLine(map[string]string{"a": "1"}, map[string]interface{}{"b": 2}, time.Now().UnixNano()+100000); err != nil {
		panic(err)
	}

	record, err := mock.ToSrvRecords()
	if err != nil {
		panic(err)
	}

	var buff []byte
	buff, err = record.Marshal(buff)
	if err != nil {
		panic(err)
	}

	client := NewWriteServiceClient(conn)
	response, err := client.WriteRows(ctx, &WriteRowsRequest{
		Version:  0,
		Database: mock.Database,
		Rp:       mock.RetentionPolicy,
		Username: "admin",
		Password: "Admin@123",
		Rows: &Rows{
			Measurement:  mock.Measurement,
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
