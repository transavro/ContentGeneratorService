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
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (s *Server) FetchHungamaPlay(_ *pb.Request, stream pb.ContentGeneratorService_FetchHungamaPlayServer) error {
	log.Print("Hit Hungama")
	var hungamalanguage = [...]string{"hindi", "english", "telugu", "kannada", "tamil", "malayalam", "punjabi", "bengali", "bhojpuri", "gujarati", "marathi", "oriya", "rajasthani"}

	var hungamaActions = [...]string{"videos", "movies", "shortfilms", "tvshow"}

	var hungamaGenre = [...]string{"Drama", "Action", "Comedy", "Romance", "Family", "Crime", "Thriller", "Musical", "Horror", "Animation", "Social",
		"Adventure", "Fantasy", "Mystery", "Mythology", "Devotional", "History", "Adult", "Awards", "Biography", "Patriotic", "Sci-Fi", "Sports", "Kids"}

	for _, action := range hungamaActions {
		for _, genre := range hungamaGenre {
			for _, lang := range hungamalanguage {

				req, err := http.NewRequest("GET", "http://affapi.hungama.com/v1/feeds/listing.json?", nil)
				if err != nil {
					return err
				}
				q := req.URL.Query()
				q.Add("action", action)
				q.Add("genre", genre)
				q.Add("lang_id", lang)
				q.Add("start", "1")
				q.Add("limit", "100")
				q.Add("country_id", "IN")
				q.Add("auth-key", "d455c1c788")

				req.URL.RawQuery = q.Encode()
				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					return err
				}

				log.Println("action ", action, "genre  ", genre, "lang  ", lang, "     ===>>>     ", resp.StatusCode)

				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}

				if resp.StatusCode != 200 {
					log.Println("not got 200 response")
					continue
				}

				// parsing json data
				var prime map[string]interface{}

				if err = json.Unmarshal(body, &prime); err != nil {
					return err
				}

				if err = resp.Body.Close(); err != nil {
					return err
				}

				if prime["status_msg"] == "success" {
					response := prime["response"].(map[string]interface{})
					switch response["data"].(type) {
					case string:
						{
							log.Println("got string ********==============>>>>")
							continue
						}
					}
					data := response["data"].([]interface{})
					for _, v := range data {
						var metadata pb.Metadata
						var media pb.Media
						tile := v.(map[string]interface{})

						// background images
						var resAry []string
						mediaSet := tile["img"].(map[string]interface{})
						if mediaSet["1024x768"] != nil && mediaSet["1024x768"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["1024x768"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["1024x768"]))
							}
						}
						if mediaSet["1280x720"] != nil && mediaSet["1280x720"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["1280x720"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["1280x720"]))
							}
						}
						media.Backdrop = resAry
						media.Banner = resAry
						if tile["preview"] != nil && tile["preview"] != "" {
							media.Video = []string{fmt.Sprint(tile["preview"])}
						} else {
							media.Video = []string{}
						}

						//portriat
						resAry = []string{}
						if mediaSet["600x415"] != nil && mediaSet["600x415"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["600x415"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["600x415"]))
							}
						}
						if mediaSet["700x394"] != nil && mediaSet["700x394"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["700x394"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["700x394"]))
							}
						}
						if mediaSet["500x500"] != nil && mediaSet["500x500"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["500x500"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["500x500"]))
							}
						}
						if mediaSet["400x400"] != nil && mediaSet["400x400"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["400x400"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["400x400"]))
							}
						}
						if mediaSet["300x300"] != nil && mediaSet["300x300"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["300x300"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["300x300"]))
							}
						}
						if mediaSet["200x200"] != nil && mediaSet["200x200"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["200x200"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["200x200"]))
							}
						}
						media.Portrait = resAry

						//landscape
						resAry = []string{}
						if mediaSet["537x768"] != nil && mediaSet["537x768"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["537x768"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["537x768"]))
							}
						}
						if mediaSet["154x220"] != nil && mediaSet["154x220"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["154x220"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["154x220"]))
							}
						}
						if mediaSet["190x273"] != nil && mediaSet["190x273"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["190x273"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["190x273"]))
							}
						}
						if mediaSet["150x210"] != nil && mediaSet["150x210"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["150x210"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["150x210"]))
							}
						}
						if mediaSet["285x135"] != nil && mediaSet["285x135"] != "" {
							if !strings.Contains(fmt.Sprint(mediaSet["285x135"]), "http://stat") {
								resAry = append(resAry, fmt.Sprint(mediaSet["285x135"]))
							}
						}
						media.Landscape = resAry

						if tile["title"] != nil && tile["title"] != "" {
							metadata.Title = strings.ToValidUTF8(fmt.Sprint(tile["title"]), "")
						} else if tile["show_name"] != nil && tile["show_name"] != "" {
							metadata.Title = strings.ToValidUTF8(fmt.Sprint(tile["show_name"]), "")
						}
						metadata.Country = []string{fmt.Sprint(tile["country"])}

						if tile["actors"] != nil && tile["actors"] != "" {
							tags := strings.Split(fmt.Sprint(tile["actors"]), ",")
							for _, tag := range tags {
								metadata.Cast = append(metadata.Cast, strings.TrimSpace(tag))
							}
						} else {
							metadata.Cast = []string{}
						}

						if tile["director"] != nil && tile["director"] != "" {
							tags := strings.Split(fmt.Sprint(tile["director"]), ",")
							for _, tag := range tags {
								metadata.Directors = append(metadata.Directors, strings.TrimSpace(tag))
							}
						} else {
							metadata.Directors = []string{}
						}

						if tile["genre"] != nil && tile["genre"] != "" {
							tags := strings.Split(fmt.Sprint(tile["genre"]), ",")
							for _, tag := range tags {
								metadata.Genre = append(metadata.Genre, strings.TrimSpace(tag))
							}
						} else {
							metadata.Genre = []string{}
						}

						if tile["language"] != nil && tile["language"] != "" {
							tags := strings.Split(fmt.Sprint(tile["language"]), ",")
							for _, tag := range tags {
								metadata.Languages = append(metadata.Languages, strings.TrimSpace(tag))
							}
						} else {
							metadata.Languages = []string{}
						}

						if tile["tags"] != nil && tile["tags"] != "" {
							tags := strings.Split(fmt.Sprint(tile["tags"]), ",")
							for _, tag := range tags {
								metadata.Tags = append(metadata.Tags, strings.TrimSpace(tag))
							}
						} else {
							metadata.Tags = []string{}
						}

						metadata.ReleaseDate = fmt.Sprint(tile["releasedate"])

						if tile["type"] != nil && tile["type"] != "" {
							tags := strings.Split(fmt.Sprint(tile["type"]), ",")
							for _, tag := range tags {
								if strings.TrimSpace(tag) == "Movie" {
									metadata.Categories = append(metadata.Categories, "Movies")
								} else if strings.TrimSpace(tag) == "Short Films" {
									metadata.Categories = append(metadata.Categories, "Short Film")
								} else {
									metadata.Categories = append(metadata.Categories, strings.TrimSpace(tag))
								}

							}
						} else {
							metadata.Categories = []string{}
						}

						if tile["nudity"] == 0 {
							metadata.KidsSafe = true
						} else {
							metadata.KidsSafe = false
						}

						if tile["duration"] != nil && tile["duration"] != "" {
							metadata.Runtime = fmt.Sprint(tile["duration"])
						}

						if tile["description"] != nil && tile["description"] != "" {
							metadata.Synopsis = strings.ToValidUTF8(fmt.Sprint(tile["description"]), "")
						}

						if tile["rating"] != 0 && tile["rating"] != nil {
							metadata.Rating = tile["rating"].(float64)
						}
						metadata.Mood = []int32{}
						n, err := strconv.ParseInt(strings.Split(strings.TrimSpace(metadata.ReleaseDate), "-")[2], 10, 32)
						metadata.Year = int32(n)
						var content pb.Content
						content.DetailPage = true
						content.PublishState = true
						content.Sources = []string{"Hungama Play"}

						//monetize
						var contentAvlb pb.ContentAvaliable
						if tile["is_rent"] != nil && tile["is_rent"] != "" {
							if tile["is_rent"] == "1" {
								contentAvlb.Monetize = pb.Monetize_Rent
							} else {
								contentAvlb.Monetize = pb.Monetize_Free
							}
						} else {
							contentAvlb.Monetize = pb.Monetize_Free
						}
						contentAvlb.Source = "Hungama Play"
						var deepLinkTarget string
						var contentId string
						switch action {
						case "tvshow":
							{
								deepLinkTarget = "tv-show"
								contentId = fmt.Sprint(tile["show_id"])
							}
						case "shortfilms":
							{
								//TODO Hungama TAkes Short film as movie in deeplink implementation
								deepLinkTarget = "movie"
								contentId = fmt.Sprint(tile["id"])
							}
						case "movies":
							{
								deepLinkTarget = "movie"
								contentId = fmt.Sprint(tile["id"])
							}
						case "videos":
							{
								deepLinkTarget = "video"
								contentId = fmt.Sprint(tile["id"])
							}
						default:
							deepLinkTarget = action
							contentId = fmt.Sprint(tile["id"])
						}

						// making deeplink
						contentAvlb.Target = s.HungamaDeadLinkMaker(deepLinkTarget, metadata.GetTitle(), contentId)
						log.Println(contentAvlb.Target)

						contentAvlb.TargetId = contentId
						contentAvlb.Package = "com.hungama.movies.tv"
						contentAvlb.Type = "CW_THIRDPARTY"

						//TODO converting hungama duration in our format.
						bytesArray, _ := GenerateRandomBytes(32)
						hasher := md5.New()
						hasher.Write(bytesArray)
						refId := hex.EncodeToString(hasher.Sum(nil))

						ts, _ := ptypes.TimestampProto(time.Now())
						log.Println(refId)

						optimus := &pb.Optimus{Metadata: &metadata, RefId: refId, Content: &content, Media: &media, CreatedAt: ts, TileType: pb.TileType_ImageTile}

						// check if already presnet
						log.Println("Checking if already present ===>   ", optimus.GetMetadata().GetTitle())
						result := s.OptimusDB.Collection("test_hungama_monetize").FindOne(context.Background(), bson.D{{"contentavailable.targetid", contentAvlb.GetTargetId()}})
						if result.Err() != nil {
							if result.Err() == mongo.ErrNoDocuments {
								log.Println("Inserting..")
								_, err = s.OptimusDB.Collection("test_hungama_content").InsertOne(context.Background(), optimus)
								if err != nil {
									return err
								}
								_, err = s.OptimusDB.Collection("test_hungama_monetize").InsertOne(context.Background(), pb.Play{
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
		}
	}
	return nil
}

func (s *Server) HungamaDeadLinkMaker(target, title, contentId string) string {
	target = strings.ToLower(target)
	reg, err := regexp.Compile("[^a-zA-Z0-9]+-")
	if err != nil {
		log.Fatal(err)
	}
	title = reg.ReplaceAllString(title, "")
	title = strings.ToLower(title)
	title = strings.Replace(title, " ", "-", -1)
	return fmt.Sprintf("http://www.hungama.com/%s/%s/%s", target, title, contentId)
}
