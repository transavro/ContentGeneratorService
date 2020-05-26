package apihandler

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/transavro/ContentGeneratorService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"strings"
	"time"
)

func (s *Server) FetchNativeData(_ *pb.Request, stream pb.ContentGeneratorService_FetchNativeDataServer) error {
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
			} else if k1 == "posters" {
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
						} else {
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
						} else {
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
						} else {
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
						} else {
							media.Backdrop = []string{}
						}
					}
				}
			} else if k1 == "content" {
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
					} else if k3 == "package" {
						if av, ok := v3.(string); ok && av != "" {
							contentAvlb.Package = av
						}
					} else if k3 == "type" {
						if av, ok := v3.(string); ok && av != "" {
							if av == "START" || av == "Start" || av == "start" {
								contentAvlb.Type = "CW_THIRDPARTY"
							} else {
								contentAvlb.Type = av
							}
						} else {
							contentAvlb.Type = "CW_THIRDPARTY"
						}
					} else if k3 == "target" {
						contentAvlb.Target = ""
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
			} else if k1 == "metadata" {
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
						} else {
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
						} else {
							metadata.Country = []string{}
						}
					} else if k4 == "rating" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Rating = float64(av)
						} else if av, ok := v4.(int32); ok && av != 0 {
							metadata.Rating = float64(av)
						} else if av, ok := v4.(int64); ok && av != 0 {
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
						} else {
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
						} else {
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
						} else {
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
								} else if categories == "Kids Rhymes" {
									metadata.Categories = append(metadata.Categories, "Kids-Rhymes")
								} else if categories == "Kid Movies" {
									metadata.Categories = append(metadata.Categories, "Kids-Movies")
								} else {
									metadata.Categories = append(metadata.Categories, strings.TrimSpace(fmt.Sprint(value)))
								}

							}
						} else {
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
						} else {
							metadata.Languages = []string{}
						}
					} else if k4 == "year" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Year = int32(av)
						} else if av, ok := v4.(int32); ok && av != 0 {
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
						} else if av, ok := v4.(int32); ok && av != 0 {
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
					} else if k4 == "part" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.Part = int32(av)
						} else if av, ok := v4.(int32); ok && av != 0 {
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
						} else if av, ok := v4.(int32); ok && av != 0 {
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
					} else if k4 == "viewCount" {
						if av, ok := v4.(int); ok && av != 0 {
							metadata.ViewCount = float64(av)
						} else if av, ok := v4.(int32); ok && av != 0 {
							metadata.ViewCount = float64(av)
						} else if av, ok := v4.(int64); ok && av != 0 {
							metadata.ViewCount = float64(av)
						} else if av, ok := v4.(float64); ok && av != 0 {
							metadata.ViewCount = av
						} else if av, ok := v4.(float32); ok && av != 0 {
							metadata.ViewCount = float64(av)
						} else {
							metadata.ViewCount = 0.0
						}
					} else if k4 == "kidsSafe" {
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
			ContentAvailable: []*pb.ContentAvaliable{&contentAvlb},
			RefId:            ref_id,
		}

		optimus = pb.Optimus{
			Media:     &media,
			RefId:     ref_id,
			TileType:  pb.TileType_ImageTile,
			Content:   &content,
			Metadata:  &metadata,
			CreatedAt: ts,
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
