package main

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	pb "github.com/transavro/ContentGeneratorService/proto"
)

func main(){
	conn, err := grpc.Dial("localhost:7780", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	client := pb.NewContentGeneratorServiceClient(conn)

	resp , err := client.FetchHungamaPlay(context.Background(), &pb.Request{})

	if err != nil {
		log.Fatal(err)
	}

	for{
		response , err := resp.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}else {
				log.Fatal(err)
			}
		}

		log.Println(response.String())
	}
	defer conn.Close()
}
