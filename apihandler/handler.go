package apihandler

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/transavro/ContentGeneratorService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// helper structs
type SchemarooCatlog struct {
	Data struct {
		CatalogListItems []struct {
			CatalogID  string `json:"catalog_id"`
			FriendlyID string `json:"friendly_id"`
		} `json:"catalog_list_items"`
	} `json:"data"`
}

type SchemarooData struct {
	Data struct {
		Items []struct {
			Title             string        `json:"title"`
			TitleWithLanguage []interface{} `json:"title_with_language"`
			ContentID         string        `json:"content_id"`
			Status            string        `json:"status"`
			CatalogID         string        `json:"catalog_id"`
			CatalogObject     struct {
				FriendlyID       string `json:"friendly_id"`
				LayoutType       string `json:"layout_type"`
				ID               string `json:"id"`
				PlanCategoryType string `json:"plan_category_type"`
				LayoutScheme     string `json:"layout_scheme"`
			} `json:"catalog_object"`
			Regions          []string      `json:"regions"`
			Language         string        `json:"language"`
			Theme            string        `json:"theme"`
			Genres           []string      `json:"genres"`
			SubGenres        []interface{} `json:"sub_genres"`
			DisplayGenres    []string      `json:"display_genres"`
			DispalySubGenres []interface{} `json:"dispaly_sub_genres"`
			Description      string        `json:"description"`
			ItemCaption      string        `json:"item_caption"`
			Thumbnails       struct {
				LMedium struct {
					URL string `json:"url"`
				} `json:"l_medium"`
				LLarge struct {
					URL string `json:"url"`
				} `json:"l_large"`
				PSmall struct {
					URL string `json:"url"`
				} `json:"p_small"`
				XlImage169 struct {
					URL string `json:"url"`
				} `json:"xl_image_16_9"`
				Large169 struct {
					URL string `json:"url"`
				} `json:"large_16_9"`
				Medium169 struct {
					URL string `json:"url"`
				} `json:"medium_16_9"`
				Small169 struct {
					URL string `json:"url"`
				} `json:"small_16_9"`
				XlImage23 struct {
					URL string `json:"url"`
				} `json:"xl_image_2_3"`
				Large23 struct {
					URL string `json:"url"`
				} `json:"large_2_3"`
				Medium23 struct {
					URL string `json:"url"`
				} `json:"medium_2_3"`
				Small23 struct {
					URL string `json:"url"`
				} `json:"small_2_3"`
				XlImage11 struct {
					URL string `json:"url"`
				} `json:"xl_image_1_1"`
				Large11 struct {
					URL string `json:"url"`
				} `json:"large_1_1"`
				Medium11 struct {
					URL string `json:"url"`
				} `json:"medium_1_1"`
				Small11 struct {
					URL string `json:"url"`
				} `json:"small_1_1"`
				XlImage165 struct {
					URL string `json:"url"`
				} `json:"xl_image_16_5"`
				Small165 struct {
					URL string `json:"url"`
				} `json:"small_16_5"`
			} `json:"thumbnails,omitempty"`
			Rating            int           `json:"rating"`
			ReleaseDate       interface{}   `json:"release_date"`
			EpisodeCount      int           `json:"episode_count"`
			EpisodeFlag       string        `json:"episode_flag"`
			SubcategoryFlag   string        `json:"subcategory_flag"`
			CustomTags        []interface{} `json:"custom_tags"`
			CatalogName       string        `json:"catalog_name"`
			LikeCount         int           `json:"like_count"`
			NoOfUserRated     int           `json:"no_of_user_rated"`
			AverageUserRating string        `json:"average_user_rating"`
			ShortDescription  string        `json:"short_description"`
			Keywords          string        `json:"keywords"`
			SequenceNo        int           `json:"sequence_no"`
			FriendlyID        string        `json:"friendly_id"`
			ViewCountFlag     string        `json:"view_count_flag"`
			DeeplinkURL       string        `json:"deeplink_url"`
			AccessControl     struct {
				IsFree bool `json:"is_free"`
			} `json:"access_control"`
		} `json:"items"`
	} `json:"data"`
}

type Server struct {
	OptimusDB *mongo.Database
	NativeTile *mongo.Collection
}

func (s *Server) FetchNativeData(request *pb.Request, stream pb.ContentGeneratorService_FetchNativeDataServer) error {
	cur, err := s.NativeTile.Find(context.Background(), bson.D{{}})
	if err != nil {
		return err
	}
	for cur.Next(context.Background()){
		var prime map[string]interface{}
		err = cur.Decode(&prime)
		if err != nil {
			return err
		}
		log.Println(prime)
	}
	return nil
}

func (s *Server) FetchJustWatch(request *pb.Request, stream pb.ContentGeneratorService_FetchJustWatchServer) error {
	return nil
}

func (s *Server) FetchHungamaPlay(request *pb.Request, stream pb.ContentGeneratorService_FetchHungamaPlayServer) error {
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

				defer resp.Body.Close()
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
				// unmarshaling byte[] to interface{}
				err = json.Unmarshal(body, &prime)
				if err != nil {
					return err
				}
				if prime["status_msg"] == "success" {
					response := prime["response"].(map[string]interface{})
					switch response["data"].(type) {
					case string:
						{
							log.Println("got string ********============== >>>>   ")
							continue
						}
					}
					data := response["data"].([]interface{})
					for _, v := range data {
						var metadata pb.Metadata
						var media pb.Media
						tile := v.(map[string]interface{})

						// background images
						resAry := []string{}
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
						}else if tile["show_name"] != nil && tile["show_name"] != "" {
							metadata.Title = strings.ToValidUTF8(fmt.Sprint(tile["show_name"]), "")
						}
						metadata.Country = fmt.Sprint(tile["country"])
						metadata.Cast = strings.Split(fmt.Sprint(tile["actors"]), ",")
						metadata.Directors = strings.Split(fmt.Sprint(tile["director"]), ",")
						metadata.Genre = strings.Split(fmt.Sprint(tile["genre"]), ",")
						metadata.Languages = strings.Split(fmt.Sprint(tile["language"]), ",")
						if tile["tags"] != nil && tile["tags"] != "" {
							metadata.Tags = strings.Split(fmt.Sprint(tile["tags"]), ",")
						} else {
							metadata.Tags = []string{}
						}
						metadata.ReleaseDate = fmt.Sprint(tile["releasedate"])
						metadata.Categories = strings.Split(fmt.Sprint(tile["type"]), ",")
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
						ref_id := hex.EncodeToString(hasher.Sum(nil))

						ts, _ := ptypes.TimestampProto(time.Now())
						log.Println(ref_id)
						optimus := &pb.Optimus{Metadata: &metadata, RefId: ref_id, Content: &content, Media: &media, CreatedAt: ts}

						// check if already presnet
						log.Println("Checking if already present ===>   ", optimus.GetMetadata().GetTitle())
						result := s.OptimusDB.Collection("test_hungama_content").FindOne(context.Background(), bson.D{{"metadata.title", optimus.GetMetadata().GetTitle()}})
						if result.Err() != nil {
							if result.Err() == mongo.ErrNoDocuments {
								log.Println("Inserting..")
								_, err = s.OptimusDB.Collection("test_hungama_content").InsertOne(context.Background(), optimus)
								if err != nil {
									return err
								}
								_, err = s.OptimusDB.Collection("test_hungama_monetize").InsertOne(context.Background(), pb.Play{
									ContentAvailable: []*pb.ContentAvaliable{&contentAvlb},
									RefId:            ref_id,
								})
								if err != nil {
									return err
								}
								log.Println("sending data to client...")
								stream.Send(optimus)
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

func (s *Server) FetchShemaroo(request *pb.Request, stream pb.ContentGeneratorService_FetchShemarooServer) error {

	req, err := http.NewRequest("GET", "https://prod.api.shemaroome.com/catalog_lists/cloudwalker-catalogs?", nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("auth_token", "5WbohMVThyftP3QGhXMs")
	q.Add("region", "IN")

	req.URL.RawQuery = q.Encode()
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var prime SchemarooCatlog
		err = json.Unmarshal(body, &prime)
		if err != nil {
			return err
		}
		for _, v := range prime.Data.CatalogListItems {
			req, err := http.NewRequest("GET", fmt.Sprintf("https://prod.api.shemaroome.com/catalogs/%s/items?", v.FriendlyID), nil)
			if err != nil {
				return err
			}
			q := req.URL.Query()
			q.Add("auth_token", "5WbohMVThyftP3QGhXMs")
			q.Add("region", "IN")
			q.Add("page_size", "40")

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

			var primeItem SchemarooData
			err = json.Unmarshal(body, &primeItem)
			if err != nil {
				return err
			}
			for _, item := range primeItem.Data.Items {
				// making metadata
				var metadata pb.Metadata
				metadata.Title = item.Title
				if item.Rating != 0 {
					metadata.Rating = float64(item.Rating)
				}
				metadata.Categories = []string{item.CatalogObject.FriendlyID, item.CatalogObject.PlanCategoryType}
				if item.Language != "" {
					metadata.Languages = []string{item.Language}
				}
				if item.ReleaseDate != "" {
					switch item.ReleaseDate.(type) {
					case int:
						{
							metadata.ReleaseDate = fmt.Sprint(item.ReleaseDate)
						}
					case string:
						{
							metadata.ReleaseDate = fmt.Sprint(item.ReleaseDate)
							n, _ := strconv.ParseInt(strings.Split(strings.TrimSpace(metadata.GetReleaseDate()), "-")[2], 10, 32)
							metadata.Year = int32(n)
						}
					}
				}


				//TODO setting default cats and director
				metadata.Cast = []string{}
				metadata.Directors = []string{}

				if item.Genres != nil {
					metadata.Genre = item.Genres
				}

				if item.Description != "" {
					metadata.Synopsis = item.Description
				}

				//TODO added default country as INDIA
				metadata.Country = "INDIA"

				if item.EpisodeCount != 0 {
					metadata.Episode = int32(item.SequenceNo)
				}

				if item.ItemCaption != "" {
					var tag = []string{}
					for _, v := range  strings.Split(item.ItemCaption, "|"){
						tag = append(tag, strings.TrimSpace(v))
					}
					metadata.Tags = tag
				}

				// creating media
				var media pb.Media
				// bg
				var resAry = []string{}
				if item.Thumbnails.XlImage11.URL != "" {
					resAry = append(resAry, item.Thumbnails.XlImage11.URL)
				}
				if item.Thumbnails.XlImage23.URL != "" {
					resAry = append(resAry, item.Thumbnails.XlImage23.URL)
				}
				if item.Thumbnails.XlImage165.URL != "" {
					resAry = append(resAry, item.Thumbnails.XlImage165.URL)
				}
				if item.Thumbnails.XlImage169.URL != "" {
					resAry = append(resAry, item.Thumbnails.XlImage169.URL)
				}
				media.Backdrop = resAry
				media.Banner = resAry

				media.Video = []string{}

				// landscape
				resAry = []string{}
				if item.Thumbnails.Large169.URL != "" {
					resAry = append(resAry, item.Thumbnails.Large169.URL)
				}
				if item.Thumbnails.Large23.URL != "" {
					resAry = append(resAry, item.Thumbnails.Large23.URL)
				}
				if item.Thumbnails.Large11.URL != "" {
					resAry = append(resAry, item.Thumbnails.Large11.URL)
				}
				if item.Thumbnails.LLarge.URL != "" {
					resAry = append(resAry, item.Thumbnails.LLarge.URL)
				}
				media.Landscape = resAry

				//portrait
				resAry = []string{}
				if item.Thumbnails.Medium11.URL != "" {
					resAry = append(resAry, item.Thumbnails.Medium11.URL)
				}
				if item.Thumbnails.Medium23.URL != "" {
					resAry = append(resAry, item.Thumbnails.Medium23.URL)
				}
				if item.Thumbnails.Medium169.URL != "" {
					resAry = append(resAry, item.Thumbnails.Medium169.URL)
				}
				if item.Thumbnails.LMedium.URL != "" {
					resAry = append(resAry, item.Thumbnails.LMedium.URL)
				}
				media.Portrait = resAry

				// making content
				var content pb.Content
				content.Sources = []string{"Shemaroo"}
				content.DetailPage = true
				content.PublishState = true

				bytesArray, _ := GenerateRandomBytes(32)
				hasher := md5.New()
				hasher.Write(bytesArray)
				ref_id := hex.EncodeToString(hasher.Sum(nil))
				ts, _ := ptypes.TimestampProto(time.Now())
				optimus := &pb.Optimus{
					Media:     &media,
					RefId:     ref_id,
					TileType:  0,
					Content:   &content,
					Metadata:  &metadata,
					CreatedAt: ts,
					UpdatedAt: nil,
				}

				var contentAvlb pb.ContentAvaliable
				if item.AccessControl.IsFree == true {
					contentAvlb.Monetize = pb.Monetize_Free
				}
				contentAvlb.Type = "CW_THIRDPARTY"
				contentAvlb.Package = "com.cloudwalker.shemarootv"
				if item.DeeplinkURL != "" {
					contentAvlb.Target = item.DeeplinkURL
				}
				contentAvlb.TargetId = item.ContentID
				contentAvlb.Source = "Shemaroo"

				// check if already presnet
				log.Println("Checking if already present ===>   ", optimus.GetMetadata().GetTitle())
				result := s.OptimusDB.Collection("test_schemaroo_content").FindOne(context.Background(), bson.D{{"metadata.title", optimus.GetMetadata().GetTitle()}})
				if result.Err() != nil {
					if result.Err() == mongo.ErrNoDocuments {
						log.Println("Inserting..")
						_, err = s.OptimusDB.Collection("test_schemaroo_content").InsertOne(context.Background(), optimus)
						if err != nil {
							return err
						}
						_, err = s.OptimusDB.Collection("test_schemaroo_monetize").InsertOne(context.Background(), pb.Play{
							ContentAvailable: []*pb.ContentAvaliable{&contentAvlb},
							RefId:            ref_id,
						})
						if err != nil {
							return err
						}
						log.Println("sending data to client...")
						stream.Send(optimus)

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

func (s *Server) FetchAltBalaji(request *pb.Request, stream pb.ContentGeneratorService_FetchAltBalajiServer) error {
	log.Print("Hit ALT BALAJI")
	return nil
}

func (s *Server) MergingOptimus(request *pb.Request, stream pb.ContentGeneratorService_MergingOptimusServer) error {
	log.Print("Hit MERGER")
	//contentCollection := s.OptimusDB.Collection("test_content")
	//hungContColl := s.OptimusDB.Collection("test_hungama_content")
	//shemaContColl := s.OptimusDB.Collection("test_schemaroo_content")





	return nil
}

func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}
	return b, nil
}
























