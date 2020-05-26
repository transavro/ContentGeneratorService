package apihandler

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/transavro/ContentGeneratorService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

func (s *Server) FetchJustWatch(_ *pb.Request, _ pb.ContentGeneratorService_FetchJustWatchServer) error {

	log.Println("JW HIT ")
	// monetize_type
	jwMonetizeType := []string{"free", "flatrate", "ads", "rent'", "buy", "5D"}
	jwProviderArray := []string{"nfx", "hoo", "prv", "hst", "voo", "viu", "jio", "zee", "ern", "itu", "ply", "mbi", "snl", "yot", "gdc", "nfk", "tbv", "ytv", "snx", "cru", "hoc", "abj"}

	jwProvidersMap := make(map[string]int)

	jwProvidersMap["nfx"] = 8
	jwProvidersMap["hoo"] = 125
	jwProvidersMap["prv"] = 119
	jwProvidersMap["hst"] = 122
	jwProvidersMap["voo"] = 121
	jwProvidersMap["viu"] = 158
	jwProvidersMap["jio"] = 220
	jwProvidersMap["zee"] = 232
	jwProvidersMap["ern"] = 218
	jwProvidersMap["itu"] = 2
	jwProvidersMap["ply"] = 3
	jwProvidersMap["mbi"] = 11
	jwProvidersMap["snl"] = 237
	jwProvidersMap["yot"] = 192
	jwProvidersMap["gdc"] = 100
	jwProvidersMap["nfk"] = 175
	jwProvidersMap["tbv"] = 73
	jwProvidersMap["ytv"] = 255
	jwProvidersMap["snx"] = 309
	jwProvidersMap["cru"] = 283
	jwProvidersMap["hoc"] = 315
	jwProvidersMap["abj"] = 319

	// JW genre
	jwGenre := make(map[string]string)
	jwGenre["1"] = "Action & Adventure"
	jwGenre["2"] = "Animation"
	jwGenre["3"] = "Comedy"
	jwGenre["4"] = "Crime"
	jwGenre["5"] = "Documentary"
	jwGenre["6"] = "Drama"
	jwGenre["7"] = "Fantasy"
	jwGenre["8"] = "History"
	jwGenre["9"] = "Horror"
	jwGenre["10"] = "Kids & Family"
	jwGenre["11"] = "Music & Musical"
	jwGenre["12"] = "Mystery & Thriller"
	jwGenre["13"] = "Romance"
	jwGenre["14"] = "Science-Fiction"
	jwGenre["15"] = "Sport & Fitness"
	jwGenre["16"] = "War & Military"
	jwGenre["17"] = "Western"

	//catrogories
	jwCategories := []string{"movie", "shows"}

	for _, provider := range jwProviderArray {

		pageCounter := 1

		values := map[string]interface{}{"monetization_types": jwMonetizeType, "page_size": 1000, "page": pageCounter, "content_types": jwCategories, "providers": []string{provider}}
		jsonValue, _ := json.Marshal(values)
		resp, err := http.Post("https://apis.justwatch.com/content/titles/en_IN/popular", "application/json", bytes.NewBuffer(jsonValue))
		if err != nil {
			return err
		}
		log.Println("response code ====>   ", resp.StatusCode)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			log.Println("not got 200 response")
		}
		// parsing json data
		var prime map[string]interface{}
		// unmarshaling byte[] to interface{}
		err = json.Unmarshal(body, &prime)
		if err != nil {
			return err
		}

		err = s.JWLogic(prime, jwProvidersMap, jwGenre)
		if err != nil {
			return err
		}

		var totalPageCount int

		switch tp := prime["total_pages"].(type) {
		case int:
			{
				totalPageCount = tp
			}
		case float32:
			{
				totalPageCount = int(tp)
			}
		}

		for i := 2; i <= totalPageCount; i++ {
			pageCounter = i
			values := map[string]interface{}{"monetization_types": jwMonetizeType, "page_size": 1000, "page": pageCounter, "content_types": jwCategories, "providers": []string{provider}}
			jsonValue, _ := json.Marshal(values)
			resp, err := http.Post("https://apis.justwatch.com/content/titles/en_IN/popular", "application/json", bytes.NewBuffer(jsonValue))
			if err != nil {
				return err
			}
			log.Println("response code ====>   ", resp.StatusCode)
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

			err = s.JWLogic(prime, jwProvidersMap, jwGenre)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (s Server) JWLogic(prime map[string]interface{}, jwProvidersMap map[string]int, jwGenre map[string]string) error {

	for _, r := range prime["items"].([]interface{}) {

		contentType := r.(map[string]interface{})["object_type"].(string)
		id := r.(map[string]interface{})["id"].(float64)

		jwUrl := fmt.Sprintf("https://apis.justwatch.com/content/titles/%s/%s/locale/en_IN", contentType, fmt.Sprint(id))

		req, err := http.NewRequest("GET", jwUrl, nil)
		if err != nil {
			return err
		}
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

		// making content
		var content pb.Content
		var contentAvalbArray []*pb.ContentAvaliable
		var metadata pb.Metadata

		content.DetailPage = false
		content.PublishState = true

		offers := prime["offers"].([]interface{})

		for _, value := range offers {
			var contentAvlb pb.ContentAvaliable
			if (prime["poster"] != nil) && (len(strings.Split(fmt.Sprint(prime["poster"]), "/")) > 2) {
				contentAvlb.TargetId = strings.Split(fmt.Sprint(prime["poster"]), "/")[2]
			}
			for mk, mv := range value.(map[string]interface{}) {
				if mk == "monetization_type" {
					switch mv {
					case "buy":
					case "flatrate":
						{
							contentAvlb.Monetize = pb.Monetize_Paid
						}
					case "rent":
						{
							contentAvlb.Monetize = pb.Monetize_Rent
						}
					}
				} else if mk == "provider_id" {
					for mapKey, mapValue := range jwProvidersMap {
						if int(mv.(float64)) == mapValue {
							contentAvlb.Source = GetSourceForJW(mapKey)
							contentAvlb.Package = GetPackageNameForJW(mapKey)
							// check if the source already present
							sourceFound := false
							for _, v := range content.Sources {
								if contentAvlb.GetSource() == v {
									sourceFound = true
									break
								}
							}
							if sourceFound == false {
								content.Sources = append(content.Sources, contentAvlb.GetSource())
							}
							if contentAvlb.GetSource() == "Youtube" {
								contentAvlb.Type = "CWYT_VIDEO"
							} else {
								contentAvlb.Type = "CW_THIRDPARTY"
							}
							break
						}
					}
				} else if mk == "urls" {
					for dk, dv := range mv.(map[string]interface{}) {
						if dk == "standard_web" {
							contentAvlb.Target = fmt.Sprint(dv)
							break
						}
					}
				} else if mk == "audio_languages" {
					isAlreadyPresent := false
					for _, val := range mv.([]interface{}) {
						log.Println(val)
						for _, lang := range metadata.GetLanguages() {
							if lang == isoCodeToLanguage(fmt.Sprint(val)) {
								isAlreadyPresent = true
								break
							}
						}
						if isAlreadyPresent == false {
							metadata.Languages = append(metadata.Languages, isoCodeToLanguage(fmt.Sprint(val)))
						}
					}
				}
			}

			// rechecking if language is filled or not
			if metadata.GetLanguages() == nil {
				metadata.Languages = []string{}
			}

			// check if already present
			contentFound := false
			for _, avaliable := range contentAvalbArray {
				if avaliable.GetSource() == contentAvlb.GetSource() {
					contentFound = true
					break
				}
			}
			if contentFound == false {
				contentAvalbArray = append(contentAvalbArray, &contentAvlb)
			}
		}

		// making media
		var media pb.Media
		if prime["clips"] != nil {
			clips := prime["clips"].([]interface{})
			// video
			for _, clip := range clips {
				for mk, mv := range clip.(map[string]interface{}) {
					if mk == "external_id" {
						media.Video = append(media.Video, fmt.Sprintf("https://www.youtube.com/watch?v=%s", mv))
					}
				}
			}
		} else {
			media.Video = []string{}
		}

		//poster
		portrait := strings.Replace(fmt.Sprint(prime["poster"]), "{profile}", "s592/movie.webp", -1)
		media.Portrait = append(media.Portrait, fmt.Sprintf("https://images.justwatch.com%s", portrait))
		//log.Println(media.Portrait)

		// backdrop , landscape
		if prime["backdrops"] != nil {
			for _, r := range prime["backdrops"].([]interface{}) {
				for mk, mv := range r.(map[string]interface{}) {
					if mk == "backdrop_url" {
						nameOfTile := fmt.Sprint(prime["full_path"])
						nameOfTile = strings.Replace(nameOfTile, "/in/movie", "", -1)
						nameOfTile = strings.Replace(nameOfTile, "/in/tv-show", "", -1)
						backdrop := strings.Replace(fmt.Sprint(mv), "{profile}", "s1440"+nameOfTile+".webp", -1)
						media.Backdrop = append(media.Backdrop, fmt.Sprintf("https://images.justwatch.com%s", backdrop))
						landscape := strings.Replace(fmt.Sprint(mv), "{profile}", "s1440"+nameOfTile+".webp", -1)
						media.Landscape = append(media.Landscape, fmt.Sprintf("https://images.justwatch.com%s", landscape))
					}
				}
			}
		} else {
			media.Backdrop = []string{}
		}

		media.Banner = []string{}

		//making metadata

		metadata.Title = fmt.Sprint(prime["title"])
		metadata.Synopsis = fmt.Sprint(prime["short_description"])
		if prime["runtime"] != nil {
			metadata.Runtime = fmt.Sprint(prime["runtime"])
		}

		// genre
		if prime["genre_ids"] != nil {
			for _, genreId := range prime["genre_ids"].([]interface{}) {
				metadata.Genre = append(metadata.Genre, jwGenre[fmt.Sprint(genreId)])
			}
		} else {
			metadata.Genre = []string{}
		}

		// cast
		if prime["credits"] != nil {
			for _, r := range prime["credits"].([]interface{}) {
				if r.(map[string]interface{})["role"] == "ACTOR" {
					metadata.Cast = append(metadata.Cast, fmt.Sprint(r.(map[string]interface{})["name"]))
				} else if r.(map[string]interface{})["role"] == "DIRECTOR" {
					metadata.Directors = append(metadata.Directors, fmt.Sprint(r.(map[string]interface{})["name"]))
				}
			}
		}

		// rechecking the cast nd director
		if len(metadata.GetCast()) == 0 {
			metadata.Cast = []string{}
		} else if len(metadata.GetDirectors()) == 0 {
			metadata.Directors = []string{}
		}

		//categories
		if prime["object_type"] != nil {
			if prime["object_type"].(string) == "movie" {
				metadata.Categories = append(metadata.Categories, "Movies")
			} else if prime["object_type"].(string) == "show" {
				metadata.Categories = append(metadata.Categories, "Tv Show")
			}
		}

		// imdbid
		if prime["scoring"] != nil {
			for _, r := range prime["scoring"].([]interface{}) {
				if r.(map[string]interface{})["provider_type"] == "tomato_userrating:meter" {
					metadata.Rating = r.(map[string]interface{})["value"].(float64)
					break
				}
			}
		}

		if prime["external_ids"] != nil {
			for _, r := range prime["external_ids"].([]interface{}) {
				if r.(map[string]interface{})["provider"] == "imdb" {
					metadata.ImdbId = fmt.Sprint(r.(map[string]interface{})["external_id"])
					break
				}
			}
		}

		//Year
		switch i := prime["original_release_year"].(type) {
		case int:
			{
				metadata.Year = int32(i)
			}
		case float64:
			{
				metadata.Year = int32(i)
			}
		case int32:
			{
				metadata.Year = i
			}
		}

		//kidsSafe
		if len(metadata.GetGenre()) > 0 {
			for _, genre := range metadata.GetGenre() {
				if genre == "Kids & Family" {
					metadata.KidsSafe = true
				} else {
					metadata.KidsSafe = false
				}
			}
		}

		metadata.Country = []string{"INDIA"}

		//relasedate
		metadata.ReleaseDate = fmt.Sprintf("05-01-%d", metadata.GetYear())

		//TAGs
		metadata.Tags = append(metadata.Tags, metadata.GetGenre()...)
		metadata.Tags = append(metadata.Tags, metadata.GetCategories()...)

		//mood
		metadata.Mood = []int32{}

		//TODO converting hungama duration in our format.
		bytesArray, _ := GenerateRandomBytes(32)
		hasher := md5.New()
		hasher.Write(bytesArray)
		ref_id := hex.EncodeToString(hasher.Sum(nil))

		ts, _ := ptypes.TimestampProto(time.Now())

		var tileType = pb.TileType_ImageTile
		if len(media.GetVideo()) > 0 {
			tileType = pb.TileType_VideoTile
		}

		optimus := &pb.Optimus{Metadata: &metadata, RefId: ref_id, Content: &content, Media: &media, CreatedAt: ts, TileType: tileType}

		//check if already presnet
		result := s.OptimusDB.Collection("test_justwatch_monetize").FindOne(context.Background(), bson.D{{"contentavailable.targetid", contentAvalbArray[0].GetTargetId()}})
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				//log.Println("Inserting..   ", optimus.GetMetadata().GetTitle())
				_, err = s.OptimusDB.Collection("test_justwatch_content").InsertOne(context.Background(), optimus)
				if err != nil {
					return err
				}
				_, err = s.OptimusDB.Collection("test_justwatch_monetize").InsertOne(context.Background(), pb.Play{
					ContentAvailable: contentAvalbArray,
					RefId:            optimus.GetRefId(),
				})
				if err != nil {
					return err
				}
			} else {
				return result.Err()
			}
		}
	}
	return nil
}

func GetSourceForJW(sourceCode interface{}) string {
	sources := fmt.Sprint(sourceCode)
	switch sources {
	case "nfx":
		{
			return "Netflix"
		}
	case "hoo":
		{
			return "Hooq"
		}
	case "prv":
		{
			return "Amazon Prime Video"
		}
	case "hst":
		{
			return "Hotstar"
		}
	case "voo":
		{
			return "Voot"
		}
	case "viu":
		{
			return "viu"
		}
	case "jio":
		{
			return "JioCinema"
		}
	case "zee":
		{
			return "ZEE5"
		}
	case "ern":
		{
			return "Eros Now"
		}
	case "itu":
		{
			return "Apple Itunes"
		}
	case "ply":
		{
			return "Google Play"
		}
	case "mbi":
		{
			return "Mubi"
		}
	case "snl":
		{
			return "Sony LIV"
		}
	case "yot":
		{
			return "Youtube"
		}
	case "gdc":
		{
			return "Guidedoc"
		}
	case "nfk":
		{
			return "Netflix Kids"
		}
	case "tbv":
		{
			return "Tube TV"
		}
	case "ytv":
		{
			return "YuppTV"
		}
	case "snx":
		{
			return "Sun NXT"
		}
	case "cru":
		{
			return "Crunchyroll"
		}
	case "hoc":
		{
			return "Hoichoi"
		}
	case "abj":
		{
			return "ALTBalaji"
		}
	}
	return ""
}


func GetPackageNameForJW(sourceCode interface{}) string {
	sources := fmt.Sprint(sourceCode)
	switch sources {
	case "nfx":
		{
			return "com.netflix.mediaclient"
		}
	case "hoo":
		{
			return "tv.hooq.android"
		}
	case "prv":
		{
			return "com.amazon.avod.thirdpartyclient"
		}
	case "hst":
		{
			return "in.startv.hotstar"
		}
	case "voo":
		{
			return "com.tv.v18.viola"
		}
	case "viu":
		{
			return "com.vuclip.viu"
		}
	case "jio":
		{
			return "com.jio.media.ondemand"
		}
	case "zee":
		{
			return "com.graymatrix.did"
		}
	case "ern":
		{
			return "com.erosnow "
		}
	case "ply":
		{
			return "com.android.vending"
		}
	case "mbi":
		{
			return "com.mubi"
		}
	case "snl":
		{
			return "com.sonyliv"
		}
	case "yot":
		{
			return "com.google.android.youtube.tv"
		}
	case "nfk":
		{
			return "com.netflix.mediaclient"
		}
	case "tbv":
		{
			return "com.tubitv"
		}
	case "ytv":
		{
			return "com.tru"
		}
	case "snx":
		{
			return "com.suntv.sunnxt"
		}
	case "cru":
		{
			return "Crunchyroll"
		}
	case "hoc":
		{
			return "com.viewlift.hoichoi"
		}
	case "abj":
		{
			return "com.balaji.alt"
		}
	}
	return ""
}

func isoCodeToLanguage(langCode string) string {
	switch langCode {
	case "en":
		{
			return "English"
		}
	case "hi":
		{
			return "Hindi"
		}
	case "ta":
		{
			return "Tamil"
		}
	case "ko":
		{
			return "Korean"
		}
	case "te":
		{
			return "Telugu"
		}
	case "ka":
		{
			return "Georgian"
		}
	case "kn":
		{
			return "Kannada"
		}
	case "mr":
		{
			return "Marathi"
		}
	case "bn":
		{
			return "Bangla"
		}
	case "ml":
		{
			return "Malayalam"
		}
	case "zh":
		{
			return "Chinese"
		}
	case "or":
		{
			return "Oriya"
		}
	case "pa":
		{
			return "Punjabi"
		}
	case "ja":
		{
			return "Japanese"
		}
	case "fr":
		{
			return "French"
		}
	}
	return ""
}
