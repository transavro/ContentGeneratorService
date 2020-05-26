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

func (s *Server) FetchShemaroo(_ *pb.Request, stream pb.ContentGeneratorService_FetchShemarooServer) error {

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

	if resp.StatusCode == 200 {

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var prime SchemarooCatlog
		if err = json.Unmarshal(body, &prime); err != nil {
			return err
		}

		if err = resp.Body.Close(); err != nil {
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
				} else if item.CatalogObject.FriendlyID == "kids-rhymes" || item.CatalogObject.PlanCategoryType == "kids-rhymes" {
					metadata.Categories = []string{"Kids-Rhymes"}
				} else if item.CatalogObject.FriendlyID == "kids-shows" || item.CatalogObject.PlanCategoryType == "kids-shows" {
					metadata.Categories = []string{"Kids-Shows"}
				} else if item.CatalogObject.FriendlyID == "bhakti" || item.CatalogObject.PlanCategoryType == "bhakti" {
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
					var tag []string
					for _, v := range strings.Split(item.ItemCaption, "|") {
						tag = append(tag, strings.TrimSpace(v))
					}
					metadata.Tags = tag
				}

				metadata.Mood = []int32{}

				// creating media
				var media pb.Media
				// bg
				var resAry []string
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

