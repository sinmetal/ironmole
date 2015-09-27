package go2bq

import (
	"fmt"
	"net/http"

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

func init() {
	http.HandleFunc("/table", handler)
	http.HandleFunc("/insert", handlerInsert)
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
