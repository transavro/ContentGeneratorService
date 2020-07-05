package apihandler

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/transavro/ContentGeneratorService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func (s *Server) FetchAltBalaji(_ *pb.Request, stream pb.ContentGeneratorService_FetchAltBalajiServer) error {
	log.Print("Hit ALT BALAJI")
	req, err := http.NewRequest("GET", "https://partners-catalog.cloud.altbalaji.com/v1/content/titleidlist?", nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("pageNo", "1")
	q.Add("pageSize", "100")
	req.URL.RawQuery = q.Encode()
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var prime map[string]interface{}
	if err = json.Unmarshal(body, &prime); err != nil {
		return err
	}

	if err = resp.Body.Close(); err != nil {
		return err
	}

	if prime["message"] == "success" {
		response := prime["data"].(map[string]interface{})
		data := response["titleIdList"]
		for _, v := range data.([]interface{}) {
			tileid := v.(map[string]interface{})
			req1, err1 := http.NewRequest("GET", "https://partners-catalog.cloud.altbalaji.com/v1/content/title/"+fmt.Sprint(tileid["id"]), nil)
			if err1 != nil {
				return err1
			}
			req1.URL.RawQuery = q.Encode()
			client1 := &http.Client{}
			resp1, err1 := client1.Do(req1)
			if err1 != nil {
				return err1
			}
			body1, err1 := ioutil.ReadAll(resp1.Body)
			if err1 != nil {
				return err1
			}

			var prime map[string]interface{}

			if err1 = json.Unmarshal(body1, &prime); err1 != nil {
				return err1
			}

			if err = resp1.Body.Close(); err != nil {
				return err
			}

			if prime["message"] == "success" {
				data := prime["data"].(map[string]interface{})
				//making metadata
				var metadata pb.Metadata
				metadata.Title = fmt.Sprint(data["title"])
				for _, cast := range data["principalCast"].([]interface{}) {
					metadata.Cast = append(metadata.Cast, fmt.Sprint(cast))
				}

				metadata.Tags = []string{fmt.Sprint(data["titleType"])}
				for _, director := range data["directors"].([]interface{}) {
					metadata.Directors = append(metadata.Directors, fmt.Sprint(director))
				}

				metadata.ReleaseDate = fmt.Sprintf("%s-%s-%s", "02", "01", data["releaseDate"])

				metadata.Synopsis = fmt.Sprint(data["description"])

				metadata.Categories = []string{"TV Series"}

				metadata.Languages = []string{}
				for _, genre := range data["genres"].([]interface{}) {
					metadata.Genre = append(metadata.Genre, fmt.Sprint(genre))
				}
				metadata.Country = []string{"INDIA"}
				metadata.Mood = []int32{}

				//media
				var media pb.Media
				media.Landscape = []string{fmt.Sprint(data["hrPosterURL"])}
				media.Backdrop = []string{fmt.Sprint(data["hrPosterURL"])}
				media.Banner = []string{fmt.Sprint(data["hrPosterURL"])}

				media.Portrait = []string{fmt.Sprint(data["vrPosterURL"])}
				media.Video = []string{}

				//conent
				var content pb.Content
				content.Sources = []string{"Alt Balaji"}
				content.PublishState = true
				content.DetailPage = true

				bytesArray, _ := GenerateRandomBytes(32)
				hasher := md5.New()
				hasher.Write(bytesArray)
				refId := hex.EncodeToString(hasher.Sum(nil))
				ts, _ := ptypes.TimestampProto(time.Now())

				optimus := &pb.Optimus{
					Media:     &media,
					RefId:     refId,
					TileType:  pb.TileType_ImageTile,
					Content:   &content,
					Metadata:  &metadata,
					CreatedAt: ts,
					UpdatedAt: nil,
				}

				// making montize
				var contentAvlb pb.ContentAvaliable
				contentAvlb.Monetize = -1
				if data["link"] != nil {
					contentAvlb.Target = fmt.Sprint(data["link"])
				} else if data["deeplink"] != nil {
					contentAvlb.Target = fmt.Sprint(data["deeplink"])
				}
				contentAvlb.Source = "Alt Balaji"
				contentAvlb.TargetId = fmt.Sprint(tileid["id"])
				contentAvlb.Package = "com.balaji.alt"
				contentAvlb.Type = "CW_THIRDPARTY"

				result := s.OptimusDB.Collection("test_altbalaji_monetize").FindOne(context.Background(), bson.D{{"contentavailable.targetid", contentAvlb.GetTargetId()}})
				if result.Err() != nil {
					if result.Err() == mongo.ErrNoDocuments {
						log.Println("Inserting..")
						_, err = s.OptimusDB.Collection("test_altbalaji_content").InsertOne(context.Background(), optimus)
						if err != nil {
							return err
						}
						_, err = s.OptimusDB.Collection("test_altbalaji_monetize").InsertOne(context.Background(), pb.Play{
							ContentAvailable: []*pb.ContentAvaliable{&contentAvlb},
							RefId:            refId,
						})
						if err != nil {
							return err
						}
						log.Println("sending data to client...")

						if err = stream.Send(optimus); err != nil {
							if err == io.EOF {
								continue
							} else {
								return err
							}
						}

					} else {
						return result.Err()
					}
				} else {
					log.Println("content already present", optimus.GetMetadata().GetTitle())
				}

			}
		}
	}
	return nil
}

