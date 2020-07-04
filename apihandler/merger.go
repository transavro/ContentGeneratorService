package apihandler

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/transavro/ContentGeneratorService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"math/rand"
	"strings"
	"time"
)

type Server struct {
	OptimusDB  *mongo.Database
	NativeTile *mongo.Collection
}

const (
	optimusDateFormat = "24-09-2009"
	nativeDateFormat  = "24 Sep 2009"
)


func (s *Server) MergingOptimus(_ *pb.Request, _ pb.ContentGeneratorService_MergingOptimusServer) error {
	log.Print("Hit MERGER")
	return s.MergingParty()
}

func (s *Server) MergingParty() error {

	//merging altbalaji content
	//hungamaContent := s.OptimusDB.Collection("test_altbalaji_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_altbalaji_monetize")

	//merging schemaroo content
	//hungamaContent := s.OptimusDB.Collection("test_schemaroo_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_schemaroo_monetize")

	//merging native content
	//hungamaContent := s.OptimusDB.Collection("test_native_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_native_monetize")

	//merging hungama content
	//hungamaContent := s.OptimusDB.Collection("test_hungama_content")
	//hungamaMonetize := s.OptimusDB.Collection("test_hungama_monetize")

	//merging JUSTWATCH content
	hungamaContent := s.OptimusDB.Collection("test_justwatch_content")
	hungamaMonetize := s.OptimusDB.Collection("test_justwatch_monetize")

	cur, err := hungamaContent.Find(context.Background(), bson.D{{}})
	if err != nil {
		return err
	}

	for cur.Next(context.Background()) {
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

	log.Println("MERging content count ==================================>     ", contentFoundCount)
	return cur.Close(context.Background())
}

var contentFoundCount = 0

func (s *Server) MergingLogic(targetOptimus pb.Optimus, play pb.Play, ctx context.Context) error {

	contentAvlb := play.ContentAvailable[0]
	baseContent := s.OptimusDB.Collection("optimus_content")
	baseMonetize := s.OptimusDB.Collection("optimus_monetize")

	// merging query
	myStages := mongo.Pipeline{}

	// first check on the bases of title
	myStages = append(myStages, bson.D{{"$match", bson.D{{"metadata.title", targetOptimus.GetMetadata().GetTitle()}}}})

	// then checking on the base of language
	myStages = append(myStages, bson.D{{"$match", bson.D{{"metadata.languages", bson.D{{"$in", targetOptimus.GetMetadata().GetLanguages()}}}}}})

	//// then checking on the base of categories
	myStages = append(myStages, bson.D{{"$match", bson.D{{"metadata.categories", bson.D{{"$in", targetOptimus.GetMetadata().GetCategories()}}}}}})

	result, err := baseContent.Aggregate(ctx, myStages)

	if err != nil {
		//TODO case 1 if the content is not Present
		if err == mongo.ErrNoDocuments {
			// found new coentent so interest it blindly
			_, err := baseContent.InsertOne(ctx, targetOptimus)
			if err != nil {
				return err
			}
			_, err = baseMonetize.InsertOne(ctx, pb.Play{
				ContentAvailable: []*pb.ContentAvaliable{contentAvlb},
				RefId:            targetOptimus.RefId,
			})
			if err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	} else {
		var noDocCounter = 0
		for result.Next(ctx) {
			noDocCounter++
			//TODO case 2 if the content is already Present
			var baseOptimus *pb.Optimus
			err := result.Decode(&baseOptimus)
			if err != nil {
				return err
			}
			contentFoundCount++
			log.Println("content Found *************  " + baseOptimus.GetRefId())
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
				} else {
					return findOneResult.Err()
				}
			} else {
				var play pb.Play
				err = findOneResult.Decode(&play)
				if err != nil {
					return err
				}
				for _, v := range play.ContentAvailable {
					if v.Source != contentAvlb.Source {
						play.ContentAvailable = append(play.ContentAvailable, contentAvlb)
						_, err = baseMonetize.ReplaceOne(ctx, montizeFilter, play)
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
				ContentAvailable: []*pb.ContentAvaliable{contentAvlb},
				RefId:            targetOptimus.RefId,
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
	rand.Seed(time.Now().UTC().UnixNano())
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}
	return b, nil
}