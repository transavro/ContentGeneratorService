package apihandler

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/transavro/ContentGeneratorService/proto"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	TilesCollection *mongo.Collection
	OptimusCollection *mongo.Collection
	MonetizeCollection *mongo.Collection
}

func(s *Server) FetchNativeData(request *pb.Request, stream pb.ContentGeneratorService_FetchNativeDataServer) error {

	return nil;
}


func(s *Server) FetchJustWatch(request *pb.Request, stream pb.ContentGeneratorService_FetchJustWatchServer) error {
	return nil;
}

func(s *Server) FetchHungamaPlay(request *pb.Request, stream pb.ContentGeneratorService_FetchHungamaPlayServer) error {
	log.Print("Hit Hungama")
	req, err := http.NewRequest("GET", "http://affapi.hungama.com/v1/feeds/listing.json?", nil)
	if err != nil {
		return  err
	}
	q := req.URL.Query()
	q.Add("action", "movies")
	q.Add("start", string(1))
	q.Add("limit", string(10))
	q.Add("country_id", "IN")
	q.Add("auth-key", "d455c1c788")

	req.URL.RawQuery = q.Encode()
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return  err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// parsing json data
	var prime map[string]interface{}

	// unmarshaling byte[] to interface{}
	err = json.Unmarshal(body, &prime)
	if err != nil {
		return err
	}
	if prime["status_msg"] == "success" {
		response := prime["response"].(map[string]interface{})
		data := response["data"].([]interface{})
		for _, v := range data {
			var metadata pb.Metadata
			var media pb.Media
			tile := v.(map[string]interface{})

			// background images
			resAry := []string{}
			mediaSet := tile["img"].(map[string]interface{})
			if mediaSet["1024x768"] != nil && mediaSet["1024x768"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["1024x768"]))
			}
			if mediaSet["1280x720"] != nil && mediaSet["1280x720"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["1280x720"]))
			}
			media.Backdrop = resAry
			media.Banner = resAry
			if tile["preview"] != nil && tile["preview"] != "" {
				media.Video	= []string{fmt.Sprint(tile["preview"])}
			}

			//portriat
			resAry = nil
			if mediaSet["600x415"] != nil && mediaSet["600x415"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["600x415"]))
			}
			if mediaSet["700x394"] != nil && mediaSet["700x394"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["700x394"]))
			}
			if mediaSet["500x500"] != nil && mediaSet["500x500"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["500x500"]))
			}
			if mediaSet["400x400"] != nil && mediaSet["400x400"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["400x400"]))
			}
			if mediaSet["300x300"] != nil && mediaSet["300x300"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["300x300"]))
			}
			if mediaSet["200x200"] != nil && mediaSet["200x200"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["200x200"]))
			}
			media.Portrait = resAry

			//landscape
			resAry = nil
			if mediaSet["537x768"] != nil && mediaSet["537x768"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["537x768"]))
			}
			if mediaSet["154x220"] != nil && mediaSet["154x220"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["154x220"]))
			}
			if mediaSet["190x273"] != nil && mediaSet["190x273"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["190x273"]))
			}
			if mediaSet["150x210"] != nil && mediaSet["150x210"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["150x210"]))
			}
			if mediaSet["285x135"] != nil && mediaSet["285x135"] != "" {
				resAry = append(resAry , fmt.Sprint(mediaSet["285x135"]))
			}
			media.Landscape = resAry





			metadata.Title = strings.ToValidUTF8(fmt.Sprint(tile["title"]), "")
			metadata.Country = fmt.Sprint(tile["country"])
			metadata.Cast = strings.Split(fmt.Sprint(tile["actors"]), ",")
			log.Println(fmt.Sprint(tile["actors"]))
			metadata.Directors = strings.Split(fmt.Sprint(tile["director"]), ",")
			metadata.Genre = strings.Split(fmt.Sprint(tile["genre"]), ",")
			metadata.Languages = strings.Split(fmt.Sprint(tile["language"]), ",")
			if tile["tags"] != nil && tile["tags"] != "" {
				metadata.Tags = strings.Split(fmt.Sprint(tile["tags"]), ",")
			}else {
				metadata.Tags = []string{}
			}
			metadata.ReleaseDate = fmt.Sprint(tile["releasedate"])
			metadata.Categories = strings.Split(fmt.Sprint(tile["type"]), ",")
			if tile["nudity"] == 0 {
				metadata.KidsSafe =  true;
			}else {
				metadata.KidsSafe = false;
			}
			metadata.Runtime = fmt.Sprint(tile["duration"])
			metadata.Synopsis = strings.ToValidUTF8(fmt.Sprint(tile["description"]), "")
			if tile["rating"] != 0 && tile["rating"] != nil {
				metadata.Rating = tile["rating"].(float64)
			}
			metadata.Mood = []int32{}
			n, err := strconv.ParseInt(strings.Split(strings.TrimSpace(metadata.ReleaseDate), "-")[2], 10, 32);
			metadata.Year = int32(n)
			var content pb.Content
			content.DetailPage = true;
			content.PublishState = true;
			content.Sources  = []string{"Hungama Play"}

			//TODO converting hungama duration in our format.
			bytes , _:= metadata.Descriptor()
			ts, _ := ptypes.TimestampProto(time.Now())
			optimus := &pb.Optimus{Metadata:&metadata, RefId: fmt.Sprint(md5.Sum(bytes)), Content: &content, Media:&media, CreatedAt:ts}
			_, err = s.OptimusCollection.InsertOne(context.Background(), optimus)
			if err != nil {
				return err
			}


			stream.Send(optimus)


			//switch v.(type) {
			//
			//case []map[string]interface{}:{
			//	log.Println("array of map")
			//}
			//case map[string]interface{}:{
			//	log.Println("map")
			//}
			//case string:{
			//	log.Println("string")
			//}
			//case int32:{
			//	log.Println("int")
			//}
			//case float64:{
			//	log.Println("float")
			//}
			//}
		}
	}


	return nil;
}

func(s *Server) FetchShemaroo(request *pb.Request, stream pb.ContentGeneratorService_FetchShemarooServer) error {
	return nil;
}

func(s *Server) FetchAltBalaji(request *pb.Request, stream pb.ContentGeneratorService_FetchAltBalajiServer) error {
	return nil;
}



