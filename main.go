package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	conn, err := grpc.Dial(":8080", grpc.WithInsecure())
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
		Rows:     &Rows{},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("response: %+v\n", response)
}
