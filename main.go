package main

import (
    "context"
    "errors"
    "fmt"
    "os"
    "time"

    "encoding/json"
    "log"
    "net/http"
    "strconv"
    "strings"

    "github.com/gorilla/handlers"
    "github.com/gorilla/mux"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/mongo/readpref"

    "github.com/shirou/gopsutil/cpu"
    "runtime"
)

const mongo_db = "moviedb"
const mongo_collection = "movies"
const mongo_default_conn_str = "mongodb://mongo-0.mongo,mongo-1.mongo,mongo-2.mongo:27017/moviedb"
const mongo_default_username = "admin"
const mongo_default_password = "password"

type moviedetail struct {
    Director    string `json:"director,omitempty" bson:"director"`
    Genre       string `json:"genre,omitempty" bson:"genre"`
    ReleaseYear int    `json:"releaseYear,omitempty" bson:"releaseYear"`
    Votes       int64  `json:"votes" bson:"votes"`
}

type movie struct {
    Title  string      `json:"title,omitempty" bson:"title"`
    Detail moviedetail `json:"moviedetail,omitempty" bson:"moviedetail"`
}

type voteresult struct {
    Title string `json:"title"`
    Votes int64  `json:"votes"`
}

var c *mongo.Client

func createMovie(w http.ResponseWriter, req *http.Request) {
    params := mux.Vars(req)
    var detail moviedetail
    _ = json.NewDecoder(req.Body).Decode(&detail)
    title := strings.ToLower(params["title"])

    fmt.Printf("POST api call made to /movies/%s\n", title)

    mov := movie{title, detail}

    id := insertNewMovie(c, mov)

    if id == nil {
        _ = json.NewEncoder(w).Encode("{'result' : 'insert failed!'}")
    } else {
        err := json.NewEncoder(w).Encode(detail)
        if err != nil {
            http.Error(w, err.Error(), 400)
        }
    }
}

func getMovies(w http.ResponseWriter, _ *http.Request) {
    fmt.Println("GET api call made to /movies")

    var movmap = make(map[string]*moviedetail)
    movs, err := returnAllMovies(c, bson.M{})

    if err != nil {
        http.Error(w, err.Error(), 400)
    }

    for _, mov := range movs {
        movmap[mov.Title] = &mov.Detail
    }

    err = json.NewEncoder(w).Encode(movmap)
    if err != nil {
        http.Error(w, err.Error(), 400)
    }
}

func getMovieByTitle(w http.ResponseWriter, req *http.Request) {
    params := mux.Vars(req)
    title := strings.ToLower(params["title"])

    fmt.Printf("GET api call made to /movies/%s\n", title)

    mov, err := returnOneMovie(c, bson.M{"title": title})
    if err != nil {
        _ = json.NewEncoder(w).Encode("{'result' : 'movie not found'}")
    } else {
        err = json.NewEncoder(w).Encode(*mov)
        if err != nil {
            http.Error(w, err.Error(), 400)
        }
    }
}

func deleteMovieByTitle(w http.ResponseWriter, req *http.Request) {
    params := mux.Vars(req)
    title := strings.ToLower(params["title"])

    fmt.Printf("DELETE api call made to /movies/%s\n", title)

    moviesRemoved := removeOneMovie(c, bson.M{"title": title})

    _ = json.NewEncoder(w).Encode(fmt.Sprintf("{'count' : %d}", moviesRemoved))
}

func voteOnMovie(w http.ResponseWriter, req *http.Request) {
    params := mux.Vars(req)
    title := strings.ToLower(params["title"])

    fmt.Printf("GET api call made to /movies/%s/vote\n", title)

    vchan := voteChannel()
    vchan <- title
    voteCount, _ := strconv.ParseInt(<-vchan, 10, 64)
    close(vchan)

    w.Header().Set("Content-Type", "application/json")

    voteresult := voteresult{
        Title: title,
        Votes: voteCount,
    }
    _ = json.NewEncoder(w).Encode(voteresult)
}

func voteChannel() (vchan chan string) {
    vchan = make(chan string)
    go func() {
        title := <-vchan
        voteUpdated := strconv.FormatInt((updateVote(c, bson.M{"title": title})), 10)
        vchan <- voteUpdated
    }()
    return vchan
}

func returnAllMovies(client *mongo.Client, filter bson.M) ([]*movie, error) {
    ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
    client.Connect(ctx)

    var movies []*movie
    collection := client.Database(mongo_db).Collection(mongo_collection)
    cur, err := collection.Find(ctx, filter)
    if err != nil {
        return nil, errors.New("error querying documents from database")
    }
    for cur.Next(context.TODO()) {
        var mov movie
        err = cur.Decode(&mov)
        if err != nil {
            return nil, errors.New("error on decoding the document")
        }
        movies = append(movies, &mov)
    }
    return movies, nil
}

func returnOneMovie(client *mongo.Client, filter bson.M) (*movie, error) {
    var mov movie
    collection := client.Database(mongo_db).Collection(mongo_collection)
    singleResult := collection.FindOne(context.TODO(), filter)
    if singleResult.Err() == mongo.ErrNoDocuments {
        return nil, errors.New("no documents found")
    }
    if singleResult.Err() != nil {
        log.Println("Find error: ", singleResult.Err())
        return nil, singleResult.Err()
    }
    singleResult.Decode(&mov)
    return &mov, nil
}

func insertNewMovie(client *mongo.Client, mov movie) interface{} {
    collection := client.Database(mongo_db).Collection(mongo_collection)
    insertResult, err := collection.InsertOne(context.TODO(), mov)
    if err != nil {
        log.Fatalln("Error on inserting new movie", err)
        return nil
    }
    return insertResult.InsertedID
}

func removeOneMovie(client *mongo.Client, filter bson.M) int64 {
    collection := client.Database(mongo_db).Collection(mongo_collection)
    deleteResult, err := collection.DeleteOne(context.TODO(), filter)
    if err != nil {
        log.Fatal("Error on deleting one movie", err)
    }
    return deleteResult.DeletedCount
}

func updateVote(client *mongo.Client, filter bson.M) int64 {
    upsert := true
    after := options.After
    opt := options.FindOneAndUpdateOptions{
        ReturnDocument: &after,
        Upsert:         &upsert,
    }

    collection := client.Database(mongo_db).Collection(mongo_collection)
    updatedData := bson.M{"$inc": bson.M{"moviedetail.votes": 1}}
    updatedResult := collection.FindOneAndUpdate(context.TODO(), filter, updatedData, &opt)
    if updatedResult.Err() != nil {
        log.Fatal("Error on updating movie vote count", updatedResult.Err())
    }
    mov := movie{}
    _ = updatedResult.Decode(&mov)
	return mov.Detail.Votes
}

//getClient returns a MongoDB Client
func getClient() *mongo.Client {
	mongoconnstr := getEnv("MONGO_CONN_STR", mongo_default_conn_str)
	mongousername := getEnv("MONGO_USERNAME", mongo_default_username)
	mongopassword := getEnv("MONGO_PASSWORD", mongo_default_password)

	fmt.Println("MongoDB connection details:")
	fmt.Println("MONGO_CONN_STR:" + mongoconnstr)
	fmt.Println("MONGO_USERNAME:" + mongousername)
	fmt.Println("MONGO_PASSWORD:")
	fmt.Println("attempting mongodb backend connection...")

	clientOptions := options.Client().ApplyURI(mongoconnstr)

	//test if auth is enabled or expected,
	//for demo purposes when we setup mongo as a replica set using a StatefulSet resource in K8s auth is disabled
	if clientOptions.Auth != nil {
		clientOptions.Auth.Username = mongousername
		clientOptions.Auth.Password = mongopassword
	}

	options.Client().SetMaxConnIdleTime(60000)
	options.Client().SetHeartbeatInterval(5 * time.Second)

	client, err := mongo.NewClient(clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func init() {
	c = getClient()
	err := c.Ping(context.Background(), readpref.Primary())
	if err != nil {
		log.Fatal("couldn't connect to the database", err)
	} else {
		log.Println("connected!!")
	}
}

func ok(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "OK!\n")
}

func version(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "API vTOKEN_VERSION\n")
}

func cpuDetails(w http.ResponseWriter, req *http.Request) {
	// cpu - get CPU number of cores and speed
	cpuStat, err := cpu.Info()
	if err != nil {
		fmt.Println(err)
	}
	
	runtimeOS := runtime.GOOS

	fmt.Fprintf(w, "OS: %s\n", runtimeOS)
	fmt.Fprintf(w, "CPU index number: %s\n", strconv.FormatInt(int64(cpuStat[0].CPU), 10))
	fmt.Fprintf(w, "CPU index number: %s\n", strconv.FormatInt(int64(cpuStat[0].CPU), 10))
	fmt.Fprintf(w, "VendorID: %s\n", cpuStat[0].VendorID)
	fmt.Fprintf(w, "Family: %s\n", cpuStat[0].Family)
	fmt.Fprintf(w, "Number of cores: %s\n", strconv.FormatInt(int64(cpuStat[0].Cores), 10))
	fmt.Fprintf(w, "Model Name: %s\n", cpuStat[0].ModelName)
	fmt.Fprintf(w, "Speed: %s\n", strconv.FormatFloat(cpuStat[0].Mhz, 'f', 2, 64))
}

func getEnv(key, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		value = fallback
	}
	return value
}

func main() {
    fmt.Println("Movie Voting App v1.0")
    fmt.Println("serving on port 8080...")
    fmt.Println("tests:")
    fmt.Println("curl -s localhost:8080/ok")
    fmt.Println("curl -s localhost:8080/cpu")
    fmt.Println("curl -s localhost:8080/version")
    fmt.Println("curl -s localhost:8080/movies")
    fmt.Println("curl -s localhost:8080/movies | jq .")

    router := mux.NewRouter()

    // Setup routes for movie endpoints
    router.HandleFunc("/movies/{title}", createMovie).Methods("POST")
    router.HandleFunc("/movies", getMovies).Methods("GET")
    router.HandleFunc("/movies/{title}", getMovieByTitle).Methods("GET")
    router.HandleFunc("/movies/{title}", deleteMovieByTitle).Methods("DELETE")
    router.HandleFunc("/movies/{title}/vote", voteOnMovie).Methods("GET")
    router.HandleFunc("/ok", ok).Methods("GET")
    router.HandleFunc("/cpu", cpuDetails).Methods("GET")
    router.HandleFunc("/version", version).Methods("GET")

    // Required for CORS
    headersOk := handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"})
    originsOk := handlers.AllowedOrigins([]string{"*"})
    methodsOk := handlers.AllowedMethods([]string{"GET", "POST", "OPTIONS"})

    // Listen on port 8080
    log.Fatal(http.ListenAndServe(":8080", handlers.CORS(originsOk, headersOk, methodsOk)(router)))
}
