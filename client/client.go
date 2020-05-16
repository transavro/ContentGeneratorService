package main

import (
	pb "github.com/transavro/ContentGeneratorService/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
)


func main() {

	conn, err := grpc.Dial("localhost:7780", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	client := pb.NewContentGeneratorServiceClient(conn)

	resp , err := client.MergingOptimus(context.Background(), &pb.Request{})

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





//
//package main
//
//import (
//
//	"fmt"
//	"log"
//
//	"strings"
//
//	"net/http"
//
//	"io/ioutil"
//
//)
//
//func main() {
//
//	//url := "https://apis.justwatch.com/content/titles/en_IN/showtimes"
//	url := "https://apis.justwatch.com/content/cinemas/en_IN"
//
//	method := "POST"
//
//	payload := strings.NewReader("{\n    \"page_size\":300,\n  \"monetization_types\": [\"free\", \"flatrate\", \"ads\", \"rent\", \"buy\", \"5D\"]\n}")
//
//	client := &http.Client {
//
//	}
//
//	req, err := http.NewRequest(method, url, payload)
//
//	if err != nil {
//
//		fmt.Println(err)
//
//	}
//
//	req.Header.Add("Content-Type", "application/json")
//
//	res, err := client.Do(req)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	defer res.Body.Close()
//
//	body, err := ioutil.ReadAll(res.Body)
//
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Println(string(body))
//
//}


