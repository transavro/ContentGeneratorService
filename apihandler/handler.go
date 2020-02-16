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
	"go.mongodb.org/mongo-driver/bson/primitive"
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

// helper structs for shemaroo
type SchemarooCatlog struct {
	Data struct {
		CatalogListItems []struct {
			CatalogID  string `json:"catalog_id,omitempty,omitempty"`
			FriendlyID string `json:"friendly_id,omitempty,omitempty"`
		} `json:"catalog_list_items,omitempty,omitempty"`
	} `json:"data,omitempty,omitempty"`
}

type SchemarooData struct {
	Data struct {
		Items []struct {
			Title             string        `json:"title,omitempty,omitempty"`
			TitleWithLanguage []interface{} `json:"title_with_language,omitempty,omitempty"`
			ContentID         string        `json:"content_id,omitempty,omitempty"`
			Status            string        `json:"status,omitempty,omitempty"`
			CatalogID         string        `json:"catalog_id,omitempty,omitempty"`
			CatalogObject     struct {
				FriendlyID       string `json:"friendly_id,omitempty,omitempty"`
				LayoutType       string `json:"layout_type,omitempty,omitempty"`
				ID               string `json:"id,omitempty,omitempty"`
				PlanCategoryType string `json:"plan_category_type,omitempty,omitempty"`
				LayoutScheme     string `json:"layout_scheme,omitempty,omitempty"`
			} `json:"catalog_object,omitempty,omitempty"`
			Regions          []string      `json:"regions,omitempty,omitempty"`
			Language         string        `json:"language,omitempty,omitempty"`
			Theme            string        `json:"theme,omitempty,omitempty"`
			Genres           []string      `json:"genres,omitempty,omitempty"`
			SubGenres        []interface{} `json:"sub_genres,omitempty,omitempty"`
			DisplayGenres    []string      `json:"display_genres,omitempty,omitempty"`
			DispalySubGenres []interface{} `json:"dispaly_sub_genres,omitempty,omitempty"`
			Description      string        `json:"description,omitempty,omitempty"`
			ItemCaption      string        `json:"item_caption,omitempty,omitempty"`
			Thumbnails       struct {
				LMedium struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"l_medium,omitempty,omitempty"`
				LLarge struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"l_large,omitempty,omitempty"`
				PSmall struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"p_small,omitempty,omitempty"`
				XlImage169 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"xl_image_16_9,omitempty,omitempty"`
				Large169 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"large_16_9,omitempty,omitempty"`
				Medium169 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"medium_16_9,omitempty,omitempty"`
				Small169 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"small_16_9,omitempty,omitempty"`
				XlImage23 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"xl_image_2_3,omitempty,omitempty"`
				Large23 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"large_2_3,omitempty,omitempty"`
				Medium23 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"medium_2_3,omitempty,omitempty"`
				Small23 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"small_2_3,omitempty,omitempty"`
				XlImage11 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"xl_image_1_1,omitempty,omitempty"`
				Large11 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"large_1_1,omitempty,omitempty"`
				Medium11 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"medium_1_1,omitempty,omitempty"`
				Small11 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"small_1_1,omitempty,omitempty"`
				XlImage165 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"xl_image_16_5,omitempty,omitempty"`
				Small165 struct {
					URL string `json:"url,omitempty,omitempty"`
				} `json:"small_16_5,omitempty,omitempty"`
			} `json:"thumbnails,omitempty,omitempty,omitempty"`
			Rating            int           `json:"rating,omitempty,omitempty"`
			ReleaseDate       interface{}   `json:"release_date,omitempty,omitempty"`
			EpisodeCount      int           `json:"episode_count,omitempty,omitempty"`
			EpisodeFlag       string        `json:"episode_flag,omitempty,omitempty"`
			SubcategoryFlag   string        `json:"subcategory_flag,omitempty,omitempty"`
			CustomTags        []interface{} `json:"custom_tags,omitempty,omitempty"`
			CatalogName       string        `json:"catalog_name,omitempty,omitempty"`
			LikeCount         int           `json:"like_count,omitempty,omitempty"`
			NoOfUserRated     int           `json:"no_of_user_rated,omitempty,omitempty"`
			AverageUserRating string        `json:"average_user_rating,omitempty,omitempty"`
			ShortDescription  string        `json:"short_description,omitempty,omitempty"`
			Keywords          string        `json:"keywords,omitempty,omitempty"`
			SequenceNo        int           `json:"sequence_no,omitempty,omitempty"`
			FriendlyID        string        `json:"friendly_id,omitempty,omitempty"`
			ViewCountFlag     string        `json:"view_count_flag,omitempty,omitempty"`
			DeeplinkURL       string        `json:"deeplink_url,omitempty,omitempty"`
			AccessControl     struct {
				IsFree bool `json:"is_free,omitempty,omitempty"`
			} `json:"access_control,omitempty,omitempty"`
		} `json:"items,omitempty,omitempty"`
	} `json:"data,omitempty,omitempty"`
}


//AltBalaji
type AltBalaji struct {
	Message   string `json:"message,omitempty"`
	Code      int    `json:"code,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Data      struct {
		Title         string   `json:"title,omitempty"`
		TitleType     string   `json:"titleType,omitempty"`
		Description   string   `json:"description,omitempty"`
		HrPosterURL   string   `json:"hrPosterURL,omitempty"`
		VrPosterURL   string   `json:"vrPosterURL,omitempty"`
		ReleaseDate   string   `json:"releaseDate,omitempty"`
		Directors     []string `json:"directors,omitempty"`
		Genres        []string `json:"genres,omitempty"`
		PrincipalCast []string `json:"principalCast,omitempty"`
		Deeplink      string   `json:"deeplink,omitempty"`
	} `json:"data,omitempty"`
}

type Server struct {
	OptimusDB  *mongo.Database
	NativeTile *mongo.Collection
}

const (
	optimusDateFormat = "24-09-2009"
	nativeDateFormat  = "24 Sep 2009"
)

func (s *Server) FetchNativeData(request *pb.Request, stream pb.ContentGeneratorService_FetchNativeDataServer) error {
	log.Println("Hit NAtive")
	cur, err := s.NativeTile.Find(stream.Context(), bson.D{{}})
	if err != nil {
		return err
	}

	for cur.Next(stream.Context()) {
		var prime primitive.D
		var media pb.Media
		var content pb.Content
		var metadata pb.Metadata
		var ref_id string
		var contentAvlb pb.ContentAvaliable
		var optimus pb.Optimus
		var play pb.Play


		err = cur.Decode(&prime)
		if err != nil {
			return err
		}


		for k1, v1 := range prime.Map() {

			if k1 == "ref_id" {
				if av, ok := v1.(string); ok && av != "" {
					ref_id = av
				}
			}else if k1 == "posters" {
				//making media
				for k2, v2 := range v1.(primitive.D).Map() {

					if k2 == "landscape" {
						media.Backdrop = []string{}
						if pa, ok := v2.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								media.Landscape = append(media.Landscape, fmt.Sprint(value))
							}
						}else {
							media.Landscape = []string{}
						}
					} else if k2 == "portrait" {
						media.Backdrop = []string{}
						if pa, ok := v2.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								media.Portrait = append(media.Portrait, fmt.Sprint(value))
							}
						}else {
							media.Portrait = []string{}
						}
					} else if k2 == "banner" {
						media.Backdrop = []string{}
						if pa, ok := v2.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								media.Banner = append(media.Banner, fmt.Sprint(value))
							}
						}else {
							media.Banner = []string{}
						}
					} else if k2 == "backdrop" {
						media.Backdrop = []string{}
						if pa, ok := v2.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								media.Backdrop = append(media.Backdrop, fmt.Sprint(value))
							}
						}else {
							media.Backdrop = []string{}
						}
					}
				}
			}else if k1 == "content" {
				// making content
				for k3, v3 := range v1.(primitive.D).Map() {
					if k3 == "source" {
						content.Sources = []string{}
						if av, ok := v3.(string); ok && av != "" {
							content.Sources = append(content.Sources, av)
							contentAvlb.Source = av
							contentAvlb.TargetId = ref_id
						}
					} else if k3 == "publishState" {
						if av, ok := v3.(bool); ok {
							content.PublishState = av
						}
					} else if k3 == "detailPage" {
						if av, ok := v3.(bool); ok {
							content.DetailPage = av
						}
					}else if k3 == "package" {
						if av, ok := v3.(string); ok && av != "" {
							contentAvlb.Package = av
						}
					}else if k3 == "type" {
						if av, ok := v3.(string); ok && av != "" {
							if av == "START" || av == "Start" || av == "start" {
								contentAvlb.Type = "CW_THIRDPARTY"
							}else {
								contentAvlb.Type = av
							}
						}else {
							contentAvlb.Type = "CW_THIRDPARTY"
						}
					}else if k3 == "target" {
						contentAvlb.Target  = ""
						if av, ok := v3.(primitive.A); ok && len(av) > 0 {
							for _, value := range av {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								contentAvlb.Target = fmt.Sprint(value)
							}
						}
					}
				}
			}else if k1 == "metadata" {
				// making metadata
				for k4, v4 := range v1.(primitive.D).Map() {
					if k4 == "title" {
						if av, ok := v4.(string); ok && av != "" {
							metadata.Title = av
						}
					} else if k4 == "customTags" {
						metadata.Tags = []string{}
						if pa, ok := v4.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								metadata.Tags = append(metadata.Tags, fmt.Sprint(value))
							}
						}else {
							metadata.Tags = []string{}
						}
					} else if k4 == "releaseDate" {
						if av, ok := v4.(string); ok && av != "" {
							metadata.ReleaseDate = av
						}
					} else if k4 == "imdbid" {
						if av, ok := v4.(string); ok && av != "" {
							metadata.ImdbId = av
						}
					} else if k4 == "synopsis" {
						if av, ok := v4.(string); ok && av != "" {
							metadata.Synopsis = av
						}
					} else if k4 == "runtime" {
						if av, ok := v4.(string); ok && av != "" {
							metadata.Runtime = av
						}
					} else if k4 == "country" {
						metadata.Country = []string{}
						if pa, ok := v4.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								metadata.Country = append(metadata.Country, strings.TrimSpace(strings.ToUpper(fmt.Sprint(value))))
							}
						}else {
							metadata.Country = []string{}
						}
					} else if k4 == "rating" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Rating = float64(av)
						}else if av, ok := v4.(int32); ok && av != 0 {
							metadata.Rating = float64(av)
						}else if av, ok := v4.(int64); ok && av != 0 {
							metadata.Rating = float64(av)
						} else if av, ok := v4.(float64); ok && av != 0 {
							metadata.Rating = av
						} else if av, ok := v4.(float32); ok && av != 0 {
							metadata.Rating = float64(av)
						} else {
							metadata.Rating = 0.0
						}
					} else if k4 == "cast" {
						metadata.Cast = []string{}
						if pa, ok := v4.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								metadata.Cast = append(metadata.Cast, strings.TrimSpace(fmt.Sprint(value)))
							}
						}else {
							metadata.Cast = []string{}
						}
					} else if k4 == "directors" {
						metadata.Directors = []string{}
						if pa, ok := v4.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								metadata.Directors = append(metadata.Directors, strings.TrimSpace(fmt.Sprint(value)))
							}
						}else {
							metadata.Directors = []string{}
						}
					} else if k4 == "genre" {
						metadata.Genre = []string{}
						if pa, ok := v4.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								metadata.Genre = append(metadata.Genre, strings.TrimSpace(fmt.Sprint(value)))
							}
						}else {
							metadata.Genre = []string{}
						}
					} else if k4 == "categories" {
						metadata.Categories = []string{}
						if pa, ok := v4.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}

								//TODO chaning categories to make the whole categories of third party at one ground.
								categories := strings.TrimSpace(fmt.Sprint(value))
								if categories == "Series" || categories == "Series with Seasons" {
									metadata.Categories = append(metadata.Categories, "TV series")
								}else if categories == "Kids Rhymes" {
									metadata.Categories = append(metadata.Categories, "Kids-Rhymes")
								}else if categories == "Kid Movies" {
									metadata.Categories = append(metadata.Categories, "Kids-Movies")
								}else {
									metadata.Categories = append(metadata.Categories, strings.TrimSpace(fmt.Sprint(value)))
								}

							}
						}else {
							metadata.Categories = []string{}
						}
					} else if k4 == "languages" {
						metadata.Languages = []string{}
						if pa, ok := v4.(primitive.A); ok && len(pa) > 0 {
							for _, value := range pa {
								if value == "N/A" || value == "n/a" || value == "null" || value == "" || value == "Null" {
									continue
								}
								metadata.Languages = append(metadata.Languages, strings.TrimSpace(fmt.Sprint(value)))
							}
						}else {
							metadata.Languages = []string{}
						}
					} else if k4 == "year" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Year = int32(av)
						}else if av, ok := v4.(int32); ok && av != 0 {
							metadata.Year = av
						} else if av, ok := v4.(int64); ok && av != 0 {
							metadata.Year = int32(av)
						} else if av, ok := v4.(float64); ok && av != 0 {
							metadata.Year = int32(av)
						} else if av, ok := v4.(float32); ok && av != 0 {
							metadata.Year = int32(av)
						} else {
							metadata.Year = 0
						}
					} else if k4 == "season" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Season = int32(av)
						}else if av, ok := v4.(int32); ok && av != 0 {
							metadata.Season = av
						} else if av, ok := v4.(int64); ok && av != 0 {
							metadata.Season = int32(av)
						} else if av, ok := v4.(float64); ok && av != 0 {
							metadata.Season = int32(av)
						} else if av, ok := v4.(float32); ok && av != 0 {
							metadata.Season = int32(av)
						} else {
							metadata.Season = 0
						}
					}  else if k4 == "part" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Part = int32(av)
						}else if av, ok := v4.(int32); ok && av != 0 {
							metadata.Part = av
						} else if av, ok := v4.(int64); ok && av != 0 {
							metadata.Part = int32(av)
						} else if av, ok := v4.(float64); ok && av != 0 {
							metadata.Part = int32(av)
						} else if av, ok := v4.(float32); ok && av != 0 {
							metadata.Part = int32(av)
						} else {
							metadata.Part = 0
						}
					} else if k4 == "episode" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Episode = int32(av)
						}else if av, ok := v4.(int32); ok && av != 0 {
							metadata.Episode = av
						} else if av, ok := v4.(int64); ok && av != 0 {
							metadata.Episode = int32(av)
						} else if av, ok := v4.(float64); ok && av != 0 {
							metadata.Episode = int32(av)
						} else if av, ok := v4.(float32); ok && av != 0 {
							metadata.Episode = int32(av)
						} else {
							metadata.Episode = 0
						}
					}else if k4 == "viewCount" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.ViewCount = float64(av)
						}else if av, ok := v4.(int32); ok && av != 0 {
							metadata.ViewCount = float64(av)
						}else if av, ok := v4.(int64); ok && av != 0 {
							metadata.ViewCount = float64(av)
						} else if av, ok := v4.(float64); ok && av != 0 {
							metadata.ViewCount = av
						} else if av, ok := v4.(float32); ok && av != 0 {
							metadata.ViewCount = float64(av)
						} else {
							metadata.ViewCount = 0.0
						}
					}else if k4 == "kidsSafe" {
						if av, ok := v4.(bool); ok {
							metadata.KidsSafe = av
						}
					}
					metadata.Mood = []int32{}
				}
			}
		}

		ts, _ := ptypes.TimestampProto(time.Now())
		contentAvlb.Monetize = -1
		media.Video = []string{}
		play = pb.Play{
			ContentAvailable:     []*pb.ContentAvaliable{&contentAvlb},
			RefId:                ref_id,
		}

		optimus = pb.Optimus{
			Media:                &media,
			RefId:                ref_id,
			TileType:             pb.TileType_ImageTile,
			Content:              &content,
			Metadata:             &metadata,
			CreatedAt:            ts,
		}



		// check if already presnet
		log.Println("Checking if already present ===>   ", optimus.GetMetadata().GetTitle())
		result := s.OptimusDB.Collection("test_native_monetize").FindOne(context.Background(), bson.D{{"metadata.title", optimus.Metadata.Title}})
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				log.Println("Inserting..")
				_, err = s.OptimusDB.Collection("test_native_content").InsertOne(context.Background(), optimus)
				if err != nil {
					return err
				}
				_, err = s.OptimusDB.Collection("test_native_monetize").InsertOne(context.Background(), play)
				if err != nil {
					return err
				}
				log.Println("sending data to client...")
				err = stream.Send(&optimus)
				if err != nil {
					return err
				}
			} else {
				return result.Err()
			}
		} else {
			log.Println("content already present", optimus.GetMetadata().GetTitle())
		}
	}
	return cur.Close(stream.Context())
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
								if strings.TrimSpace(tag) == "Movie"{
									metadata.Categories = append(metadata.Categories, "Movies" )
								}else if strings.TrimSpace(tag) == "Short Films"{
									metadata.Categories = append(metadata.Categories, "Short Film" )
								}else {
									metadata.Categories = append(metadata.Categories, strings.TrimSpace(tag) )
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
						ref_id := hex.EncodeToString(hasher.Sum(nil))

						ts, _ := ptypes.TimestampProto(time.Now())
						log.Println(ref_id)
						optimus := &pb.Optimus{Metadata: &metadata, RefId: ref_id, Content: &content, Media: &media, CreatedAt: ts, TileType: pb.TileType_ImageTile}

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

				if item.CatalogObject.FriendlyID == "kids-movie" || item.CatalogObject.PlanCategoryType == "kids-movie" {
					metadata.Categories = []string{"Kids-Movies"}
				}else if   item.CatalogObject.FriendlyID == "kids-rhymes" || item.CatalogObject.PlanCategoryType == "kids-rhymes" {
					metadata.Categories = []string{"Kids-Rhymes"}
				} else if  item.CatalogObject.FriendlyID == "kids-shows" || item.CatalogObject.PlanCategoryType == "kids-shows" {
					metadata.Categories = []string{"Kids-Shows"}
				}else if  item.CatalogObject.FriendlyID == "bhakti" || item.CatalogObject.PlanCategoryType == "bhakti" {
					metadata.Categories = []string{"Devotional Videos"}
				} else {
					metadata.Categories = []string{item.CatalogObject.FriendlyID}
				}

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
				metadata.Country = []string{"INDIA"}

				if item.EpisodeCount != 0 {
					metadata.Episode = int32(item.SequenceNo)
				}

				if item.ItemCaption != "" {
					var tag = []string{}
					for _, v := range strings.Split(item.ItemCaption, "|") {
						tag = append(tag, strings.TrimSpace(v))
					}
					metadata.Tags = tag
				}

				metadata.Mood = []int32{}

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
					TileType:  pb.TileType_ImageTile,
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
				result := s.OptimusDB.Collection("test_schemaroo_monetize").FindOne(context.Background(), bson.D{{"contentavailable.targetid", contentAvlb.GetTargetId()}})
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
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var prime map[string]interface{}
	err = json.Unmarshal(body, &prime)
	if err != nil {
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
			defer resp1.Body.Close()
			body1, err1 := ioutil.ReadAll(resp1.Body)
			if err1 != nil {
				return err1
			}

			var altbaljivar *AltBalaji
			err1 = json.Unmarshal(body1, &altbaljivar)
			if err1 != nil {
				return err1
			}

			log.Println(altbaljivar.Data.Title, "   ================    ", fmt.Sprint(tileid["id"]))

			//making metadata
			var metadata pb.Metadata
			metadata.Title = altbaljivar.Data.Title
			metadata.Cast = altbaljivar.Data.PrincipalCast
			metadata.Tags = []string{altbaljivar.Data.TitleType}
			metadata.Directors = altbaljivar.Data.Directors
			metadata.ReleaseDate = fmt.Sprintf("%s-%s-%s", "02", "01", altbaljivar.Data.ReleaseDate)
			metadata.Synopsis = altbaljivar.Data.Description
			metadata.Categories = []string{altbaljivar.Data.TitleType}
			metadata.Languages = []string{}
			metadata.Genre = altbaljivar.Data.Genres
			metadata.Country = []string{"INDIA"}
			metadata.Mood = []int32{}

			//media
			var media pb.Media
			media.Landscape = []string{altbaljivar.Data.HrPosterURL}
			media.Backdrop = []string{altbaljivar.Data.HrPosterURL}
			media.Banner = []string{altbaljivar.Data.HrPosterURL}

			media.Portrait = []string{altbaljivar.Data.VrPosterURL}
			media.Video = []string{}

			//conent
			var content pb.Content
			content.Sources = []string{"Alt Balaji"}
			content.PublishState = true
			content.DetailPage = true

			bytesArray, _ := GenerateRandomBytes(32)
			hasher := md5.New()
			hasher.Write(bytesArray)
			ref_id := hex.EncodeToString(hasher.Sum(nil))
			ts, _ := ptypes.TimestampProto(time.Now())

			optimus := &pb.Optimus{
				Media:     &media,
				RefId:     ref_id,
				TileType:  pb.TileType_ImageTile,
				Content:   &content,
				Metadata:  &metadata,
				CreatedAt: ts,
				UpdatedAt: nil,
			}

			// making montize
			var contentAvlb pb.ContentAvaliable
			contentAvlb.Monetize = -1
			contentAvlb.Target = altbaljivar.Data.Deeplink
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
	return nil
}

func (s *Server) MergingOptimus(request *pb.Request, stream pb.ContentGeneratorService_MergingOptimusServer) error {
	log.Print("Hit MERGER")
	return s.MergingParty()
}

func (s *Server) MergingParty() error {

	//merging altbalaji content
	hungamaContent := s.OptimusDB.Collection("test_altbalaji_content")
	hungamaMonetize := s.OptimusDB.Collection("test_altbalaji_monetize")

	//merging schemaroo content
	//hungamaContent := s.OptimusDB.Collection("test_schemaroo_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_schemaroo_monetize")

	//merging native content
	//hungamaContent := s.OptimusDB.Collection("test_native_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_native_monetize")

	//merging hungama content
	//hungamaContent := s.OptimusDB.Collection("test_hungama_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_hungama_monetize")

	cur, err := hungamaContent.Find(context.Background(), bson.D{{}})
	if err != nil {
		return err
	}


	for cur.Next(context.Background()){
		var optimus pb.Optimus
		var play pb.Play
		err = cur.Decode(&optimus)
		if err != nil {
			return err
		}
		result := hungamaMonetize.FindOne(context.Background(), bson.D{{"refid", optimus.GetRefId()}})
		if result.Err() != nil {
			return result.Err()
		}
		err = result.Decode(&play)
		if err != nil {
			return err
		}

		err = s.MergingLogic(optimus, play, context.Background())
		if err != nil {
			return err
		}
	}

	log.Println("MERging content count ==================================>     ",contentFoundCount)
	cur.Close(context.Background())
	return nil
}

var contentFoundCount = 0;

func (s *Server) MergingLogic(targetOptimus pb.Optimus, play pb.Play, ctx context.Context) error {

	contentAvlb := play.ContentAvailable[0]
	baseContent := s.OptimusDB.Collection("optimus_content")
	baseMonetize := s.OptimusDB.Collection("optimus_monetize")

	// merging query
	myStages := mongo.Pipeline{}

	// first check on the bases of title
	myStages = append(myStages, bson.D{{"$match",  bson.D{{"metadata.title",  targetOptimus.GetMetadata().GetTitle()}}}})

	// then checking on the base of language
	myStages = append(myStages,bson.D{{"$match",  bson.D{{"metadata.languages", bson.D{{"$in", targetOptimus.GetMetadata().GetLanguages()}}}}}})

	//// then checking on the base of categories
	myStages = append(myStages,bson.D{{"$match",  bson.D{{"metadata.categories", bson.D{{"$in", targetOptimus.GetMetadata().GetCategories()}}}}}})

	result , err := baseContent.Aggregate(ctx, myStages,)

	if err != nil || result.Err() != nil {
		//TODO case 1 if the content is not Present
		log.Println("got error ", err, result.Err())
		if err == mongo.ErrNoDocuments ||  result.Err() == mongo.ErrNoDocuments {
			// found new coentent so interest it blindly
			_, err := baseContent.InsertOne(ctx, targetOptimus)
			if err != nil {
				return err
			}
			_, err = baseMonetize.InsertOne(ctx, pb.Play{
				ContentAvailable:     []*pb.ContentAvaliable{contentAvlb},
				RefId:                targetOptimus.RefId,
			})
			if err != nil {
				return err
			}
			return nil
		}else {
			return result.Err()
		}
	} else   {
		var noDocCounter = 0
		for result.Next(ctx){
			noDocCounter++
			//TODO case 2 if the content is already Present
			var baseOptimus *pb.Optimus
			err := result.Decode(&baseOptimus)
			if err != nil {
				return err
			}
			contentFoundCount ++
			log.Println("content Found *************  "+ baseOptimus.GetRefId())
			// starting with media comparsion

			// ladscape
			if len(baseOptimus.GetMedia().GetLandscape()) > 0 {
				for _, s2 := range targetOptimus.GetMedia().GetLandscape() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMedia().GetLandscape() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMedia().Landscape = append(baseOptimus.GetMedia().Landscape, s2)
					}
				}
			} else {
				baseOptimus.GetMedia().Landscape = targetOptimus.GetMedia().GetLandscape()
			}

			// portrait
			if len(baseOptimus.GetMedia().GetPortrait()) > 0 {
				for _, s2 := range targetOptimus.GetMedia().GetPortrait() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMedia().GetPortrait() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMedia().Portrait = append(baseOptimus.GetMedia().Portrait, s2)
					}
				}
			} else {
				baseOptimus.GetMedia().Portrait = targetOptimus.GetMedia().GetPortrait()
			}

			// backdrop
			if len(baseOptimus.GetMedia().GetBackdrop()) > 0 {
				for _, s2 := range targetOptimus.GetMedia().GetBackdrop() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMedia().GetBackdrop() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMedia().Backdrop = append(baseOptimus.GetMedia().Backdrop, s2)
					}
				}
			} else {
				baseOptimus.GetMedia().Backdrop = targetOptimus.GetMedia().GetBackdrop()
			}

			//banner
			if len(baseOptimus.GetMedia().GetBanner()) > 0 {
				for _, s2 := range targetOptimus.GetMedia().GetBanner() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMedia().GetBanner() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMedia().Banner = append(baseOptimus.GetMedia().Banner, s2)
					}
				}
			} else {
				baseOptimus.GetMedia().Banner = targetOptimus.GetMedia().GetBanner()
			}

			//Video
			if len(baseOptimus.GetMedia().GetVideo()) > 0 {
				for _, s2 := range targetOptimus.GetMedia().GetVideo() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMedia().GetVideo() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMedia().Video = append(baseOptimus.GetMedia().Video, s2)
					}
				}
			} else {
				baseOptimus.GetMedia().Video = targetOptimus.GetMedia().GetVideo()
			}

			// set the Tile type if the video url is avaliable
			if len(baseOptimus.GetMedia().GetVideo()) > 0 {
				baseOptimus.TileType = pb.TileType_VideoTile
			}

			// content Part
			if len(baseOptimus.GetContent().GetSources()) > 0 {
				for _, s2 := range targetOptimus.GetContent().GetSources() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetContent().GetSources() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetContent().Sources = append(baseOptimus.GetContent().Sources, s2)
					}
				}
			}

			// metadata part

			// imdb
			if targetOptimus.GetMetadata().GetImdbId() != "" {
				baseOptimus.GetMetadata().ImdbId = targetOptimus.GetMetadata().GetImdbId()
			}

			//sysnopsis
			if targetOptimus.GetMetadata().GetSynopsis() != "" {
				baseOptimus.GetMetadata().Synopsis = targetOptimus.GetMetadata().GetSynopsis()
			}

			//Country
			if len(baseOptimus.GetMetadata().GetCountry()) > 0 {
				for _, s2 := range targetOptimus.GetMetadata().GetCountry() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMetadata().GetCountry() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMetadata().Country = append(baseOptimus.GetMetadata().Country, s2)
					}
				}
			}


			//runtime
			if targetOptimus.GetMetadata().GetRuntime() != "" {
				baseOptimus.GetMetadata().Runtime = targetOptimus.GetMetadata().GetRuntime()
			}

			//rating
			if targetOptimus.GetMetadata().GetRating() != 0.0 {
				baseOptimus.GetMetadata().Rating = targetOptimus.GetMetadata().GetRating()
			}

			//releaseDate
			if targetOptimus.GetMetadata().GetReleaseDate() != "" {
				baseOptimus.GetMetadata().ReleaseDate = targetOptimus.GetMetadata().GetReleaseDate()
			}

			//Tags
			if len(baseOptimus.GetMetadata().GetTags()) > 0 {
				for _, s2 := range targetOptimus.GetMetadata().GetTags() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMetadata().GetTags() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMetadata().Tags = append(baseOptimus.GetMetadata().Tags, s2)
					}
				}
			}


			//Year
			if targetOptimus.GetMetadata().GetYear() != 0 && baseOptimus.GetMetadata().GetYear() == 0 {
				baseOptimus.GetMetadata().Year = targetOptimus.GetMetadata().Year
			}

			//cast
			if len(baseOptimus.GetMetadata().GetCast()) > 0 {
				for _, s2 := range targetOptimus.GetMetadata().GetCast() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMetadata().GetCast() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMetadata().Cast = append(baseOptimus.GetMetadata().Cast, s2)
					}
				}
			}

			//director
			if len(baseOptimus.GetMetadata().GetDirectors()) > 0 {
				for _, s2 := range targetOptimus.GetMetadata().GetDirectors() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMetadata().GetDirectors() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMetadata().Directors = append(baseOptimus.GetMetadata().Directors, s2)
					}
				}
			}

			//genre
			if len(baseOptimus.GetMetadata().GetGenre()) > 0 {
				for _, s2 := range targetOptimus.GetMetadata().GetGenre() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMetadata().GetGenre() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMetadata().Genre = append(baseOptimus.GetMetadata().Genre, s2)
					}
				}
			}

			// categories
			if len(baseOptimus.GetMetadata().GetCategories()) > 0 {
				for _, s2 := range targetOptimus.GetMetadata().GetCategories() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMetadata().GetCategories() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMetadata().Categories = append(baseOptimus.GetMetadata().Categories, s2)
					}
				}
			}

			// languages
			if len(baseOptimus.GetMetadata().GetLanguages()) > 0 {
				for _, s2 := range targetOptimus.GetMetadata().GetLanguages() {
					var isPresent bool
					for _, s3 := range baseOptimus.GetMetadata().GetLanguages() {
						if strings.EqualFold(s2, s3) {
							isPresent = true
						}
					}
					if isPresent == false {
						baseOptimus.GetMetadata().Languages = append(baseOptimus.GetMetadata().Languages, s2)
					}
				}
			}

			//kidsSafe
			baseOptimus.GetMetadata().KidsSafe = targetOptimus.GetMetadata().KidsSafe

			//viewCount TODO alag game hai iska Please keep a note of it. ****************************************
			if baseOptimus.GetMetadata().ViewCount == 0.0 {
				baseOptimus.GetMetadata().ViewCount = targetOptimus.GetMetadata().ViewCount
			}

			//season
			if baseOptimus.GetMetadata().GetSeason() == 0 {
				baseOptimus.GetMetadata().Season = targetOptimus.GetMetadata().GetSeason()
			}

			//episode
			if baseOptimus.GetMetadata().GetEpisode() == 0 {
				baseOptimus.GetMetadata().Episode = targetOptimus.GetMetadata().GetEpisode()
			}

			//Part
			if baseOptimus.GetMetadata().GetPart() == 0 {
				baseOptimus.GetMetadata().Part = targetOptimus.GetMetadata().GetPart()
			}

			//mood TODO check the login of it in future. Subjected to change **************************
			if len(baseOptimus.GetMetadata().GetMood()) > 0 {
				baseOptimus.GetMetadata().Mood = targetOptimus.GetMetadata().Mood
			}
			ts, _ := ptypes.TimestampProto(time.Now())
			baseOptimus.UpdatedAt = ts


			_, err = baseContent.ReplaceOne(ctx, bson.D{{"refid", baseOptimus.GetRefId()}}, baseOptimus)
			if err != nil {
				return err
			}

			// making monetize

			// case 1 if the content is not present
			montizeFilter := bson.D{{"refid", baseOptimus.GetRefId()}}

			findOneResult := baseMonetize.FindOne(ctx, montizeFilter)
			if findOneResult.Err() != nil {
				if findOneResult.Err() == mongo.ErrNoDocuments {
					log.Fatal("+++++++++++++++++++++++    Worng Logic ")
				}else {
					return findOneResult.Err()
				}
			}else {
				var play pb.Play
				err = findOneResult.Decode(&play)
				if err != nil {
					return err
				}
				for _, v := range play.ContentAvailable {
					if v.Source != contentAvlb.Source {
						play.ContentAvailable = append(play.ContentAvailable, contentAvlb)
						_ , err = baseMonetize.ReplaceOne(ctx, montizeFilter, play)
						if err != nil {
							return err
						}
						break
					}
				}
			}
		}
		if noDocCounter == 0 {
			// found new content so inserting it blindly
			_, err := baseContent.InsertOne(ctx, targetOptimus)
			if err != nil {
				return err
			}
			_, err = baseMonetize.InsertOne(ctx, pb.Play{
				ContentAvailable:     []*pb.ContentAvaliable{contentAvlb},
				RefId:                targetOptimus.RefId,
			})
			if err != nil {
				return err
			}
			return nil
		}
	}

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








