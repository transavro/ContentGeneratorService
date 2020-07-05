package apihandler

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/transavro/ContentGeneratorService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// necessity param required to run humgama api

var (
	hungamalanguage = [...]string{"hindi", "english", "telugu", "kannada", "tamil", "malayalam", "punjabi", "bengali", "bhojpuri", "gujarati", "marathi", "oriya", "rajasthani"}

	hungamaActions = [...]string{"movies", "shortfilms", "tvshow"}

	hungamaGenre = [...]string{"Drama", "Action", "Comedy", "Romance", "Family", "Crime", "Thriller", "Musical", "Horror", "Animation", "Social",
		"Adventure", "Fantasy", "Mystery", "Mythology", "Devotional", "History", "Adult", "Awards", "Biography", "Patriotic", "Sci-Fi", "Sports", "Kids"}
)

func (e *Server) FetchHungamaPlay(_ *pb.Request, stream pb.ContentGeneratorService_FetchHungamaPlayServer) error {
	// taken all pointer to optimize memory consumption and not to create new obj every time.
	var (
		err         error
		client      *http.Client
		req         *http.Request
		resp        *http.Response
		media       *pb.Media
		content     *pb.Content
		metadata    *pb.Metadata
		optimus     *pb.Optimus
		contentAvlb *pb.ContentAvaliable
		ref_id 		*string
	)

	//giving an address to the pointers
	client = &http.Client{}
	req = &http.Request{}
	resp = &http.Response{}
	prime := make(map[string]interface{}) // maps are always internally a pointer.
	metadata = &pb.Metadata{}
	media = &pb.Media{}
	content = &pb.Content{}
	optimus = &pb.Optimus{}
	contentAvlb = &pb.ContentAvaliable{}
	tmpRef := ""
	ref_id = &tmpRef


	// Implementing nested for loop
	// looping from action // first loop
	for _, action := range hungamaActions {

		for _, genre := range hungamaGenre {

			for _, lang := range hungamalanguage {

				if req, err = makingReq(action, genre, lang); err != nil { // making request to hit
					return err
				} else if resp, err = makingApiCall(client, req); err != nil { // hitting api call to hungama
					return err
				} else if resp.StatusCode != 200 {
					// if some url by chance give error code other than 200 lets not stuck so continue.
					continue
				} else if err = makingJsonMap(resp, prime); err != nil { // converting resp to json map
					return err
				} else {
					// here we finally got the map here we r gonna get data from. // playing with json map here
					e.preparingData(prime, action, contentAvlb, media, content, metadata, optimus, ref_id, stream) // need to look at it. // later
				}
			}
		}
	}
	return nil
}

// preparing data from hungama api
func (e *Server) preparingData(
	prime map[string]interface{},
	action string,
	contentAvlb *pb.ContentAvaliable,
	media *pb.Media,
	content *pb.Content,
	metadata *pb.Metadata,
	optimus *pb.Optimus,
	ref_id *string,
	stream pb.ContentGeneratorService_FetchHungamaPlayServer,
) {

	// lets first check if the resp is 200 but the data fetch is succeful or not
	if prime["status_msg"] != "success" {
		// if the response is not sucessful then skip
		return
	}

	// getting response obj
	response := prime["response"].(map[string]interface{})

	// checking the data type of the variable
	switch response["data"].(type) {
	case string:
		return
	}

	data := response["data"].([]interface{})
	// looping from tile array
	for _, tile := range data {
		contentAvlb.Reset()
		media.Reset()
		content.Reset()
		metadata.Reset()
		e.makingTileObj(tile.(map[string]interface{}), action, contentAvlb, media, content, metadata, optimus,ref_id, stream) // type casting it to map[string]interface{}
	}
}

func (e *Server) makingTileObj(
	tile map[string]interface{},
	action string,
	contentAvlb *pb.ContentAvaliable,
	media *pb.Media,
	content *pb.Content,
	metadata *pb.Metadata,
	optimus *pb.Optimus,
	ref_id *string,
	stream pb.ContentGeneratorService_FetchHungamaPlayServer,
) {
	// preparing tile obj
	returnFlag := false
	if tile["type"] != nil && tile["type"] != "" {
		tags := strings.Split(fmt.Sprint(tile["type"]), ",")
		for _, tag := range tags {
			if strings.TrimSpace(tag) == "Movie Videos" {
				returnFlag = true
			} else if strings.TrimSpace(tag) == "Events and Broadcasts Video" {
				returnFlag = true
			} else if strings.TrimSpace(tag) == "Music Video Track" {
				returnFlag = true
			}
		}
	}
	if returnFlag {
		return
	}

	//making media
	makingMedia(tile, media)
	//making metadata
	makingMetaData(tile, metadata)
	//making content
	makingContent(content)
	// making content Avaliable
	makingContentAvlb(tile, action, metadata.GetTitle(), contentAvlb)
	//making optimus
	*ref_id = makingRefId()
	makingOptimus(optimus, media, content, metadata, ref_id)
	// Add if already present in DB
	addHungamaNotInDB(e.OptimusDB.Collection("nayan_hungama_content"),
		e.OptimusDB.Collection("nayan_hungama_monetize"),
		contentAvlb,
		optimus,
		stream,
	)
}

func makingOptimus(
	optimus *pb.Optimus,
	media *pb.Media,
	content *pb.Content,
	metadata *pb.Metadata,
	ref_id *string) {

	ts, _ := ptypes.TimestampProto(time.Now())
	optimus.Metadata = metadata
	optimus.Content = content
	optimus.Media = media
	optimus.CreatedAt = ts
	optimus.RefId = *ref_id
	optimus.TileType = pb.TileType_ImageTile
}

func makingMedia(tile map[string]interface{}, media *pb.Media) {

	mediaSet := tile["img"].(map[string]interface{})

	// images
	for k, v := range mediaSet {
		if strings.Contains(fmt.Sprintf("%v", v), "http://stat"){
			continue
		}
		// remove spaces from the string
		k = strings.TrimSpace(k)
		var w, h int
		tmp := strings.Split(k, "x")
		if len(tmp) == 0 {
			continue
		}
		w, _  = strconv.Atoi(tmp[0])
		h, _  = strconv.Atoi(tmp[1])

		// if width is greater than thounsand it of bg and banner type
		if w > 1000 {
			// adding in bg and banner array
			if media.Backdrop == nil {
				media.Backdrop = []string{fmt.Sprintf("%v", v)}
			}else {
				media.Backdrop = append(media.Backdrop, fmt.Sprintf("%v", v))
			}

			if media.Banner == nil {
				media.Banner = []string{fmt.Sprintf("%v", v)}
			}else {
				media.Banner = append(media.Banner, fmt.Sprintf("%v", v))
			}

		} else {
			// check if the image is landscape , logic is if w > h
			if w > h {
				if media.Landscape == nil {
					media.Landscape = []string{fmt.Sprintf("%v", v)}
				}else {
					media.Landscape = append(media.Landscape, fmt.Sprintf("%v", v))
				}
			}else{
				if media.Portrait == nil {
					media.Portrait = []string{fmt.Sprintf("%v", v)}
				}else {
					media.Portrait = append(media.Portrait, fmt.Sprintf("%v", v))
				}
			}
		}
	}


	// recheck for null and setting it to empty value
	if media.GetPortrait() == nil  || len(media.GetPortrait()) == 0 {
		media.Portrait = []string{}
	}
	if media.GetLandscape() == nil || len(media.GetLandscape()) == 0 {
		media.Landscape = []string{}
	}
	if media.GetBackdrop() == nil || len(media.GetBackdrop()) == 0 {
		media.Backdrop = []string{}
	}
	if media.GetBanner() == nil || len(media.GetBanner()) == 0 {
		media.Banner = []string{}
	}


	// video
	if tile["preview"] != nil && tile["preview"] != "" {
		media.Video = []string{fmt.Sprint(tile["preview"])}
	} else {
		media.Video = []string{}
	}
}

func makingMetaData(tile map[string]interface{}, metadata *pb.Metadata) () {

	// not yet used or decided.
	metadata.Mood = []int32{}

	// making title
	if tile["title"] != nil && tile["title"] != "" {
		metadata.Title = strings.ToValidUTF8(fmt.Sprint(tile["title"]), "")
	} else if tile["show_name"] != nil && tile["show_name"] != "" {
		metadata.Title = strings.ToValidUTF8(fmt.Sprint(tile["show_name"]), "")
	} else {
		metadata.Title = ""
	}

	//making country
	if tile["country"] != nil && tile["country"] != "" {
		metadata.Country = []string{fmt.Sprint(tile["country"])}
	} else {
		metadata.Country = []string{}
	}

	//making cast
	if tile["actors"] != nil && tile["actors"] != "" {
		actors := strings.Split(fmt.Sprint(tile["actors"]), ",")
		for _, actor := range actors {
			metadata.Cast = append(metadata.Cast, strings.TrimSpace(actor))
		}
	} else {
		metadata.Cast = []string{}
	}

	//making director
	if tile["director"] != nil && tile["director"] != "" {
		directors := strings.Split(fmt.Sprint(tile["director"]), ",")
		for _, d := range directors {
			metadata.Directors = append(metadata.Directors, strings.TrimSpace(d))
		}
	} else {
		metadata.Directors = []string{}
	}

	// making genre
	if tile["genre"] != nil && tile["genre"] != "" {
		tags := strings.Split(fmt.Sprint(tile["genre"]), ",")
		for _, tag := range tags {
			metadata.Genre = append(metadata.Genre, strings.TrimSpace(tag))
		}
	} else {
		metadata.Genre = []string{}
	}

	// making lang
	if tile["language"] != nil && tile["language"] != "" {
		tags := strings.Split(fmt.Sprint(tile["language"]), ",")
		for _, tag := range tags {
			metadata.Languages = append(metadata.Languages, strings.TrimSpace(tag))
		}
	} else {
		metadata.Languages = []string{}
	}

	// making tags
	if tile["tags"] != nil && tile["tags"] != "" {
		tags := strings.Split(fmt.Sprint(tile["tags"]), ",")
		for _, tag := range tags {
			metadata.Tags = append(metadata.Tags, strings.TrimSpace(tag))
		}
	} else {
		metadata.Tags = []string{}
	}

	// making release date
	if tile["releasedate"] != nil && tile["releasedate"] != "" {
		metadata.ReleaseDate = fmt.Sprint(tile["releasedate"])
		if n, err := strconv.ParseInt(strings.Split(strings.TrimSpace(metadata.ReleaseDate), "-")[2], 10, 32); err == nil {
			metadata.Year = 0000
		} else {
			metadata.Year = int32(n)
		}
	} else {
		metadata.ReleaseDate = ""
		metadata.Year = 0000
	}

	// making content Type
	if tile["type"] != nil && tile["type"] != "" {
		tags := strings.Split(fmt.Sprint(tile["type"]), ",")
		for _, tag := range tags {
			if strings.TrimSpace(tag) == "Movie" {
				metadata.Categories = append(metadata.Categories, "Movies")
			} else if strings.TrimSpace(tag) == "Short Films" {
				metadata.Categories = append(metadata.Categories, "Short Film")
			} else if strings.TrimSpace(tag) == "TV Series" {
				metadata.Categories = append(metadata.Categories, "TV Series")
			}
		}
	} else {
		metadata.Categories = []string{}
	}

	// is kids Safe
	if tile["nudity"] == 0 {
		metadata.KidsSafe = true
	} else {
		metadata.KidsSafe = false
	}

	/// making duration
	if tile["duration"] != nil && tile["duration"] != "" {
		metadata.Runtime = fmt.Sprint(tile["duration"])
	} else {
		metadata.Runtime = ""
	}

	// making description
	if tile["description"] != nil && tile["description"] != "" {
		metadata.Synopsis = strings.ToValidUTF8(fmt.Sprint(tile["description"]), "")
	} else {
		metadata.Synopsis = ""
	}

	// making rating
	if tile["rating"] != 0 && tile["rating"] != nil {
		metadata.Rating = tile["rating"].(float64)
	} else {
		metadata.Rating = 0.0
	}
}

func makingContent(content *pb.Content) () {
	content.DetailPage = true
	content.PublishState = true
	content.Sources = []string{"Hungama Play"}
}

func makingContentAvlb(tile map[string]interface{}, action, title string, contentAvlb *pb.ContentAvaliable) {
	// monetize type
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

	// making target
	tmp := deeplinkMaker(action, title, tile)
	contentAvlb.Target = tmp[0]
	contentAvlb.TargetId = tmp[1]
	contentAvlb.Package = "com.hungama.movies.tv"
	contentAvlb.Type = "CW_THIRDPARTY"
}

func deeplinkMaker(action, title string, tile map[string]interface{}) (result []string) {
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
	result = append(result, hungamaDeadLinkMaker(deepLinkTarget, title, contentId))
	result = append(result, contentId)
	return result
}

func makingRefId() string {
	bytesArray, _ := GenerateRandomBytes(32)
	hasher := md5.New()
	hasher.Write(bytesArray)
	return hex.EncodeToString(hasher.Sum(nil))
}


// perfectly done dont touch it
func addHungamaNotInDB(
	hungamaContentColl,
	hungamaMonetizeColl *mongo.Collection,
	contentAvlb *pb.ContentAvaliable,
	optimus *pb.Optimus,
	stream pb.ContentGeneratorService_FetchHungamaPlayServer,
) (err error) {

	result := hungamaMonetizeColl.FindOne(context.Background(), bson.D{{"contentavailable.targetid", contentAvlb.GetTargetId()}})

	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			_, err = hungamaContentColl.InsertOne(context.Background(), optimus)
			if err != nil {
				return err
			}
			_, err = hungamaMonetizeColl.InsertOne(context.Background(), pb.Play{
				ContentAvailable: []*pb.ContentAvaliable{contentAvlb},
				RefId:            optimus.GetRefId(),
			})
			if err != nil {
				return err
			}
			if err = stream.Send(optimus); err != nil && err != io.EOF {
				return err
			}
		} else {
			return result.Err()
		}
	}
	return nil
}

func hungamaDeadLinkMaker(target, title, contentId string) string {
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

// NOTE: API Call Set of Helper Methods
// making hungama request
func makingReq(action, genre, lang string) (*http.Request, error) {
	req, err := http.NewRequest("GET", "http://affapi.hungama.com/v1/feeds/listing.json?", nil)
	if err != nil {
		return nil, err
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
	return req, nil
}

//making hungama api call
func makingApiCall(client *http.Client, req *http.Request) (resp *http.Response, err error) {
	log.Println("###############################################################################")
	log.Println(req)
	log.Println("#################################################################################")
	if resp, err = client.Do(req); err != nil {
		return nil, err
	}
	return resp, nil
}

// making json obj
func makingJsonMap(resp *http.Response, result map[string]interface{}) error {
	if body, err := ioutil.ReadAll(resp.Body); err != nil {
		return err
	} else if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	return resp.Body.Close()
}

// TODO Algo

// dry run karto correct me if i am wrong

// 1 hiting the api by changing the genre action and lang in for loop one by one
// 2 taking the data and converting it to our object
// 3 check if the data is already present in mongo
// 4 if not present the store the data.
// correct ?
