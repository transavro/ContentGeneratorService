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


// native cats helper strcut
type NativeContent struct {
	Media struct {
		Landscape []string      `json:"landscape"`
		Portrait  []string      `json:"portrait"`
		Backdrop  []string      `json:"backdrop"`
		Banner    []string      `json:"banner"`
		Video     []interface{} `json:"video"`
	} `json:"media"`
	Refid    string `json:"refid"`
	Tiletype int    `json:"tiletype"`
	Content  struct {
		Publishstate bool     `json:"publishstate"`
		Detailpage   bool     `json:"detailpage"`
		Sources      []string `json:"sources"`
	} `json:"content"`
	Metadata struct {
		Title       string        `json:"title"`
		Imdbid      string        `json:"imdbid"`
		Synopsis    string        `json:"synopsis"`
		Country     []string      `json:"country"`
		Runtime     string        `json:"runtime"`
		Rating      int           `json:"rating"`
		Releasedate string        `json:"releasedate"`
		Tags        interface{}   `json:"tags"`
		Year        int           `json:"year"`
		Cast        []string      `json:"cast"`
		Directors   []string      `json:"directors"`
		Genre       []string      `json:"genre"`
		Categories  []string      `json:"categories"`
		Languages   []string `json:"languages"`
		Kidssafe    bool          `json:"kidssafe"`
		Viewcount   int           `json:"viewcount"`
		Season      int           `json:"season"`
		Episode     int           `json:"episode"`
		Part        int           `json:"part"`
	} `json:"metadata"`
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
	OptimusDB *mongo.Database
	NativeTile *mongo.Collection
}

func (s *Server) FetchNativeData(request *pb.Request, stream pb.ContentGeneratorService_FetchNativeDataServer) error {
	log.Println("Hit NAtive")
	cur, err := s.OptimusDB.Collection("nativecontents").Find(context.Background(), bson.D{{}})
	if err !=  nil {
		return err
	}

	defer  cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var nativeCats NativeContent
		err = cur.Decode(&nativeCats)
		if err != nil {
			log.Println("error while decoding ")
			return err
		}
		
		// media 
		var Media pb.Media
		if len(nativeCats.Media.Landscape) > 0 {
			for _, v := range nativeCats.Media.Landscape {
				Media.Landscape = append(Media.Landscape, fmt.Sprint(v))
			}
		}
		if len(nativeCats.Media.Portrait) > 0 {
			for _, v := range nativeCats.Media.Portrait {
				Media.Portrait = append(Media.Portrait, fmt.Sprint(v))
			}
		}
		if len(nativeCats.Media.Backdrop) > 0 {
			for _, v := range nativeCats.Media.Backdrop {
				Media.Backdrop = append(Media.Backdrop, fmt.Sprint(v))
			}
		}
		if len(nativeCats.Media.Banner) > 0 {
			for _, v := range nativeCats.Media.Banner {
				Media.Banner = append(Media.Banner, fmt.Sprint(v))
			}
		}
		if len(nativeCats.Media.Video) > 0 {
			for _, v := range nativeCats.Media.Video {
				Media.Video = append(Media.Video, fmt.Sprint(v))
			}
		}

		// content
		var Content pb.Content
		Content.PublishState = nativeCats.Content.Publishstate
		Content.DetailPage = nativeCats.Content.Detailpage
		Content.Sources = nativeCats.Content.Sources

		//Metadata
		var Metadata pb.Metadata
		Metadata.Title = nativeCats.Metadata.Title
		Metadata.ImdbId = nativeCats.Metadata.Imdbid
		Metadata.Synopsis = nativeCats.Metadata.Synopsis
		Metadata.Country = nativeCats.Metadata.Country
		Metadata.Runtime = nativeCats.Metadata.Runtime
		//if nativeCats.Metadata.Rating != 0 {
		//	switch i := nativeCats.Metadata.Rating.(type) {
		//	case int:
		//		{
		//			Metadata.Rating = float64(i)
		//		}
		//	case float32:
		//		{
		//			Metadata.Rating = float64(i)
		//		}
		//	case float64:
		//		{
		//			Metadata.Rating = i
		//		}
		//	}
		//}

		Metadata.ReleaseDate = nativeCats.Metadata.Releasedate
		Metadata.Country = nativeCats.Metadata.Country

		//if nativeCats.Metadata.Viewcount != 0 {
		//	switch i := nativeCats.Metadata.Viewcount.(type) {
		//	case int:
		//		{
		//			Metadata.ViewCount = float64(i)
		//		}
		//	case float32:
		//		{
		//			Metadata.ViewCount = float64(i)
		//		}
		//	case float64:
		//		{
		//			Metadata.ViewCount = i
		//		}
		//	}
		//}

		Metadata.KidsSafe = nativeCats.Metadata.Kidssafe
		Metadata.Cast = nativeCats.Metadata.Cast
		Metadata.Directors = nativeCats.Metadata.Directors
		Metadata.Categories = nativeCats.Metadata.Categories
		Metadata.Languages = nativeCats.Metadata.Languages
		//if nativeCats.Metadata.Year != 0 {
		//	Metadata.Year = int32(nativeCats.Metadata.Year)
		//}
		//Metadata.Season = int32(nativeCats.Metadata.Season)
		//Metadata.Episode = int32(nativeCats.Metadata.Season)
		//Metadata.Part = int32(nativeCats.Metadata.Season)
		//if len(nativeCats.Metadata.Mood) > 0 {
		//	for _, v := range nativeCats.Metadata.Mood {
		//		Metadata.Mood = append(Metadata.Mood, v.(int32))
		//	}
		//}
		
		err = stream.Send(&pb.Optimus{
			Media:                &Media,
			RefId:                nativeCats.Refid,
			TileType:             pb.TileType_ImageTile,
			Content:              &Content,
			Metadata:             &Metadata,
		})
		if err != nil {
			return err
		}
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
						metadata.Country = []string{fmt.Sprint(tile["country"])}
						metadata.Cast = strings.Split(strings.TrimSpace(fmt.Sprint(tile["actors"])), ",")
						metadata.Directors = strings.Split(fmt.Sprint(tile["director"]), ",")
						metadata.Genre = strings.Split(fmt.Sprint(tile["genre"]), ",")
						metadata.Languages = strings.Split(fmt.Sprint(tile["language"]), ",")
						if tile["tags"] != nil && tile["tags"] != "" {
							metadata.Tags = strings.Split(strings.TrimSpace(fmt.Sprint(tile["tags"])), ",")
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
						optimus := &pb.Optimus{Metadata: &metadata, RefId: ref_id, Content: &content, Media: &media, CreatedAt: ts, TileType:pb.TileType_ImageTile}

						// check if already presnet
						log.Println("Checking if already present ===>   ", optimus.GetMetadata().GetTitle())
						result := s.OptimusDB.Collection("test_hungama_monetize").FindOne(context.Background(), bson.D{{"contentavailable.targetid", optimus.GetMetadata().GetTitle()}})
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
				metadata.Country = []string{"INDIA"}

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
				result := s.OptimusDB.Collection("test_schemaroo_monetize").FindOne(context.Background(), bson.D{{"contentavailable.targetid", optimus.Metadata.Title}})
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

			log.Println(altbaljivar.Data.Title ,"   ================    " ,fmt.Sprint(tileid["id"]))

			//making metadata
			var metadata pb.Metadata
			metadata.Title = altbaljivar.Data.Title
			metadata.Cast = altbaljivar.Data.PrincipalCast
			metadata.Directors = altbaljivar.Data.Directors
			metadata.ReleaseDate = fmt.Sprintf("%s-%s-%s","02","01",altbaljivar.Data.ReleaseDate)
			metadata.Synopsis = altbaljivar.Data.Description
			metadata.Categories = []string{altbaljivar.Data.TitleType}
			metadata.Languages = []string{}
			metadata.Genre = altbaljivar.Data.Genres
			metadata.Country = []string{"INDIA"}

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
				Media:                &media,
				RefId:                ref_id,
				TileType:             pb.TileType_ImageTile,
				Content:              &content,
				Metadata:             &metadata,
				CreatedAt:            ts,
				UpdatedAt:            nil,
			}

			// making montize
			var contentAvlb pb.ContentAvaliable
			contentAvlb.Monetize = -1
			contentAvlb.Target = altbaljivar.Data.Deeplink
			contentAvlb.Source = "Alt Balaji"
			contentAvlb.TargetId = fmt.Sprint(tileid["id"])
			contentAvlb.Package = "com.balaji.alt"
			contentAvlb.Type = "CW_THIRDPARTY"

			result := s.OptimusDB.Collection("test_altbalaji_monetize").FindOne(context.Background(), bson.D{{"contentavailable.targetid", optimus.GetMetadata().GetTitle()}})
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



	return nil
}


func (s *Server) MergingNative(){





}

func (s *Server) MergingHungama() error {
	// base content where all tiles are stored.
	//baseContent := s.OptimusDB.Collection("base_content")
	//baseMonetize := s.OptimusDB.Collection("base_monetize")
	//
	////merging hungama content
	//hungamaContent := s.OptimusDB.Collection("test_hungama_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_hungama_monetize")

	//cur, err := hungamaContent.Find(context.Background(), bson.D{{}})
	//if err != nil {
	//	return err
	//}
	//
	//var optimus *pb.Optimus
	//for cur.Next(context.Background()){
	//	err = cur.Decode(&optimus)
	//	if err != nil {
	//		return err
	//	}
	//
	//}

	return nil
}

func (s *Server) MergingLogic(targetOptimus pb.Optimus, targetMonetize pb.Monetize , ctx context.Context) error {


	
	baseContent := s.OptimusDB.Collection("base_content")
	baseMonetize := s.OptimusDB.Collection("base_monetize")

	queryFilter :=  bson.D{{"$and", bson.A{bson.D{{"metadata.title", targetOptimus.GetMetadata().GetTitle()}},
		bson.D{{"content.source", targetOptimus.GetContent().GetSources()[0] }}   }}}

	
	result := baseContent.FindOne(ctx, queryFilter)

	
	if result.Err() != nil {
		//TODO case 1 if the content is not Present
		if result.Err() == mongo.ErrNoDocuments {
			// found new coentent so interest it blindly
			_, err := baseContent.InsertOne(ctx, targetOptimus)
			if err != nil {
				return err
			}
			_, err = baseMonetize.InsertOne(ctx, targetMonetize)
			if err != nil {
				return err
			}
			return nil
		}
	}else {
		//TODO case 2 if the content is already Present
		var baseOptimus *pb.Optimus
		err := result.Decode(baseOptimus)
		if err != nil {
			return err
		}

		// starting with media comparsion

		// ladscape
		if len(baseOptimus.GetMedia().GetLandscape()) > 0 {
			for _, v := range targetOptimus.GetMedia().GetLandscape() {
				// check if already present, if not add
				for _, j := range baseOptimus.GetMedia().GetLandscape() {
					if v == j {
						break
					}else {
						baseOptimus.GetMedia().Landscape = append(baseOptimus.GetMedia().Landscape , v)
					}
				}
			}
		}else {
			baseOptimus.GetMedia().Landscape = targetOptimus.GetMedia().GetLandscape()
		}


		// portrait
		if len(baseOptimus.GetMedia().GetPortrait()) > 0 {
			for _, v := range targetOptimus.GetMedia().GetPortrait() {
				// check if already present, if not add
				for _, j := range baseOptimus.GetMedia().GetPortrait() {
					if v == j {
						break
					}else {
						baseOptimus.GetMedia().Portrait = append(baseOptimus.GetMedia().Portrait , v)
					}
				}
			}
		}else {
			baseOptimus.GetMedia().Portrait = targetOptimus.GetMedia().GetPortrait()
		}


		// backdrop
		if len(baseOptimus.GetMedia().GetBackdrop()) > 0 {
			for _, v := range targetOptimus.GetMedia().GetBackdrop() {
				// check if already present, if not add
				for _, j := range baseOptimus.GetMedia().GetBackdrop() {
					if v == j {
						break
					}else {
						baseOptimus.GetMedia().Backdrop = append(baseOptimus.GetMedia().Backdrop , v)
					}
				}
			}
		}else {
			baseOptimus.GetMedia().Backdrop = targetOptimus.GetMedia().GetBackdrop()
		}


		//banner
		if len(baseOptimus.GetMedia().GetBanner()) > 0 {
			for _, v := range targetOptimus.GetMedia().GetBanner() {
				// check if already present, if not add
				for _, j := range baseOptimus.GetMedia().GetBanner() {
					if v == j {
						break
					}else {
						baseOptimus.GetMedia().Banner = append(baseOptimus.GetMedia().Banner , v)
					}
				}
			}
		}else {
			baseOptimus.GetMedia().Banner = targetOptimus.GetMedia().GetBanner()
		}

		//Video
		if len(baseOptimus.GetMedia().GetVideo()) > 0 {
			for _, v := range targetOptimus.GetMedia().GetVideo() {
				// check if already present, if not add
				for _, j := range baseOptimus.GetMedia().GetVideo() {
					if v == j {
						break
					}else {
						baseOptimus.GetMedia().Video = append(baseOptimus.GetMedia().Video , v)
					}
				}
			}
		}else {
			baseOptimus.GetMedia().Video = targetOptimus.GetMedia().GetVideo()
		}



		// set the Tile type if the video url is avaliable
		if len(baseOptimus.GetMedia().GetVideo()) > 0 {
			baseOptimus.TileType = pb.TileType_VideoTile
		}

		// content Part
		for _, v := range targetOptimus.GetContent().GetSources() {
			for _, j := range baseOptimus.GetContent().GetSources() {
				if v == j {
					break
				}else {
					baseOptimus.GetContent().Sources = append(baseOptimus.GetContent().Sources, v)
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
		if len(targetOptimus.GetMetadata().GetCountry()) > 0 {
			for _, v := range targetOptimus.GetMetadata().GetCountry() {
				for _, j := range baseOptimus.GetMetadata().GetCountry() {
					if v == j {
						break
					}else {
						baseOptimus.GetMetadata().Country  = append(baseOptimus.GetMetadata().Country, v)
					}
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
		if len(targetOptimus.GetMetadata().GetTags()) > 0 {
			for _, v := range targetOptimus.GetMetadata().GetTags() {
				for _, j := range baseOptimus.GetMetadata().GetTags() {
					if v == j {
						break
					}else {
						baseOptimus.GetMetadata().Tags  = append(baseOptimus.GetMetadata().Tags, v)
					}
				}
			}
		}


		//Year
		if targetOptimus.GetMetadata().GetYear() != 0 && baseOptimus.GetMetadata().GetYear() == 0 {
			baseOptimus.GetMetadata().Year = targetOptimus.GetMetadata().Year
		}


		//cast
		if len(targetOptimus.GetMetadata().GetCast()) > 0 {
			for _, v := range targetOptimus.GetMetadata().GetCast() {
				for _, j := range baseOptimus.GetMetadata().GetCast() {
					if v == j {
						break
					}else {
						baseOptimus.GetMetadata().Cast  = append(baseOptimus.GetMetadata().Cast, v)
					}
				}
			}
		}

		//director
		if len(targetOptimus.GetMetadata().GetDirectors()) > 0 {
			for _, v := range targetOptimus.GetMetadata().GetDirectors() {
				for _, j := range baseOptimus.GetMetadata().GetDirectors() {
					if v == j {
						break
					}else {
						baseOptimus.GetMetadata().Directors  = append(baseOptimus.GetMetadata().Directors, v)
					}
				}
			}
		}


		//genre
		if len(targetOptimus.GetMetadata().GetGenre()) > 0 {
			for _, v := range targetOptimus.GetMetadata().GetGenre() {
				for _, j := range baseOptimus.GetMetadata().GetGenre() {
					if v == j {
						break
					}else {
						baseOptimus.GetMetadata().Genre  = append(baseOptimus.GetMetadata().Genre, v)
					}
				}
			}
		}

		// categories
		if len(targetOptimus.GetMetadata().GetCategories()) > 0 {
			for _, v := range targetOptimus.GetMetadata().GetCategories() {
				for _, j := range baseOptimus.GetMetadata().GetCategories() {
					if v == j {
						break
					}else {
						baseOptimus.GetMetadata().Categories  = append(baseOptimus.GetMetadata().Categories, v)
					}
				}
			}
		}

		// languages
		if len(targetOptimus.GetMetadata().GetLanguages()) > 0 {
			for _, v := range targetOptimus.GetMetadata().GetLanguages() {
				for _, j := range baseOptimus.GetMetadata().GetLanguages() {
					if v == j {
						break
					}else {
						baseOptimus.GetMetadata().Languages  = append(baseOptimus.GetMetadata().Languages, v)
					}
				}
			}
		}

		//kidsSafe
		baseOptimus.GetMetadata().KidsSafe = targetOptimus.GetMetadata().KidsSafe

		//viewCount TODO alag game hai iska Please keep a not of it. ****************************************
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
		_, err = baseContent.ReplaceOne(ctx, queryFilter, baseOptimus)
		if err != nil {
			return err
		}


		// making monetize


		// case 1 if the content is not present
		//montizeFilter = bson.D{{""}}

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
























