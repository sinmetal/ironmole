package go2bq

import (
	"fmt"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"

	bigquery "google.golang.org/api/bigquery/v2"
)

type Container struct {
	Hoge Hoge
	Fuga Hoge
}

type Container2 struct {
	Hoge Hoge
	Fuga Hoge
	Key  *datastore.Key
	pri  int
}

type Hoge struct {
	Name string
	Age  int
	//pri  int
}

// Item
type Item struct {
	KeyStr    string    `json:"key" datastore:"-"`
	Title     string    `json:"title" datastore:",noindex"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Moge
type Moge struct {
	KeyStr    string         `json:"key" datastore:"-"`
	Title     string         `json:"title" datastore:",noindex"`
	ItemKey   *datastore.Key `json:"itemKey"`
	Item      Item           `json:"item"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

func init() {
	http.HandleFunc("/table", handler)
	http.HandleFunc("/insert", handlerInsert)
	http.HandleFunc("/tableMoge", handlerTableMoge)
	http.HandleFunc("/insertMoge", handlerInsertMoge)
}

func handler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(ctx, bigquery.BigqueryScope),
			Base:   &urlfetch.Transport{Context: ctx},
		},
	}

	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Errorf("%v", err)
	}

	key := datastore.Key{}
	c := Container2{
		Hoge: Hoge{Name: "hoge", Age: 28},
		Key:  &key,
	}
	schema := make([]*bigquery.TableFieldSchema, 0, 10)
	schema = BuildSchema(schema, "", c)

	err = CreateTable(bq, "cp300demo1", "go2bq", "Container2", schema)
	if err != nil {
		log.Errorf(ctx, "%v", err)
	}
}

func handlerInsert(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(ctx, bigquery.BigqueryScope),
			Base:   &urlfetch.Transport{Context: ctx},
		},
	}

	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Errorf("%v", err)
	}

	key := datastore.Key{}
	c := Container2{
		Hoge: Hoge{Name: "hoge", Age: 28},
		Key:  &key,
	}

	jsonValue := make(map[string]bigquery.JsonValue)
	BuildJsonValue(jsonValue, "", c)
	res, err := Insert2(bq, "cp300demo1", "go2bq", "Container2", jsonValue)
	if err != nil {
		log.Errorf(ctx, "%v", err)
	}
	for _, insertError := range res.InsertErrors {
		for _, error := range insertError.Errors {
			log.Errorf(ctx, "Insert Error = %v", error)
		}
	}
}

func handlerTableMoge(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(ctx, bigquery.BigqueryScope),
			Base:   &urlfetch.Transport{Context: ctx},
		},
	}

	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Errorf("%v", err)
	}

	moge := Moge{}
	schema := make([]*bigquery.TableFieldSchema, 0, 10)
	schema = BuildSchema(schema, "", moge)

	err = CreateTable(bq, "cp300demo1", "go2bq", "Moge", schema)
	if err != nil {
		log.Errorf(ctx, "%v", err)
	}
}

func handlerInsertMoge(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(ctx, bigquery.BigqueryScope),
			Base:   &urlfetch.Transport{Context: ctx},
		},
	}

	bq, err := bigquery.New(client)
	if err != nil {
		fmt.Errorf("%v", err)
	}

	keyStr := r.URL.Query().Get("key")

	key, err := datastore.DecodeKey(keyStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var moge Moge
	err = datastore.Get(ctx, key, &moge)
	if err == datastore.ErrNoSuchEntity {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	moge.KeyStr = key.Encode()

	jsonValue := make(map[string]bigquery.JsonValue)
	BuildJsonValue(jsonValue, "", moge)
	res, err := Insert2(bq, "cp300demo1", "go2bq", "Moge", jsonValue)
	if err != nil {
		log.Errorf(ctx, "%v", err)
	}
	for _, insertError := range res.InsertErrors {
		for _, error := range insertError.Errors {
			log.Errorf(ctx, "Insert Error = %v", error)
		}
	}
}
