package example

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"

	"github.com/sinmetal/ironmole/v0/ironmole"
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
	pri  int
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
	KeyStr    string           `json:"key" datastore:"-"`
	Key       *datastore.Key   `json:"-" datastore:"-"`
	Title     string           `json:"title" datastore:",noindex"`
	ItemKey   *datastore.Key   `json:"itemKey"`
	ItemKeys  []*datastore.Key `json:"itemKeys"`
	Item      Item             `json:"item"`
	Refs      []string         `json:"refs"`
	CreatedAt time.Time        `json:"createdAt"`
	UpdatedAt time.Time        `json:"updatedAt"`
}

func init() {
	http.HandleFunc("/tableContainer2", handlerContainer2)
	http.HandleFunc("/insert", handlerInsert)
	http.HandleFunc("/tableMoge", handlerTableMoge)
	http.HandleFunc("/insertMoge", handlerInsertMoge)
	http.HandleFunc("/moge", handlerMoge)
}

func handlerContainer2(w http.ResponseWriter, r *http.Request) {
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
	schema, err := ironmole.BuildTableSchema(&c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = ironmole.CreateTable(bq, "cp300demo1", "go2bq", "Container2", schema)
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

	jsonValue, err := ironmole.BuildJsonValue(&c)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := ironmole.Insert(bq, "cp300demo1", "go2bq", "Container2", jsonValue)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, insertError := range res.InsertErrors {
		for _, err := range insertError.Errors {
			log.Errorf(ctx, "Insert Error = %v", err)
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
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	table := "Moge"
	tableParam := r.FormValue("table")
	if len(tableParam) > 0 {
		table = tableParam
	}

	moge := Moge{}
	schema, err := ironmole.BuildTableSchemaWithContext(ctx, &moge)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = ironmole.CreateTable(bq, "cp300demo1", "go2bq", table, schema)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("done"))
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	keyStr := r.FormValue("key")
	key, err := datastore.DecodeKey(keyStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	table := "Moge"
	tableParam := r.FormValue("table")
	if len(tableParam) > 0 {
		table = tableParam
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
	moge.Key = key
	moge.KeyStr = key.Encode()

	jsonValue, err := ironmole.BuildJsonValueWithContext(ctx, &moge)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := ironmole.Insert(bq, "cp300demo1", "go2bq", table, jsonValue)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, insertError := range res.InsertErrors {
		for _, error := range insertError.Errors {
			log.Errorf(ctx, "Insert Error = %v", error)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("done"))
}

func handlerMoge(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	q := datastore.NewQuery("Moge").Limit(10)

	var items []Moge
	t := q.Run(ctx)
	for {
		var item Moge
		k, err := t.Next(&item)
		if err == datastore.Done {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		item.KeyStr = k.Encode()
		items = append(items, item)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(items)
}

func (m *Moge) BuildTableSchema(schema []*bigquery.TableFieldSchema) ([]*bigquery.TableFieldSchema, error) {
	schema = append(schema, &bigquery.TableFieldSchema{
		Name: "__INSERT_ID__",
		Type: "STRING",
	})
	schema = append(schema, &bigquery.TableFieldSchema{
		Name: "__INSERT_DATE__",
		Type: "TIMESTAMP",
	})

	return schema, nil
}

func (m *Moge) BuildTableSchemaWithContext(ctx context.Context, schema []*bigquery.TableFieldSchema) ([]*bigquery.TableFieldSchema, error) {
	log.Infof(ctx, "Moge = %v", m)

	schema = append(schema, &bigquery.TableFieldSchema{
		Name: "__INSERT_ID__",
		Type: "STRING",
	})
	schema = append(schema, &bigquery.TableFieldSchema{
		Name: "__INSERT_DATE__",
		Type: "TIMESTAMP",
	})
	log.Infof(ctx, "Moge Schema = %v", schema)

	return schema, nil
}

func (m *Moge) BuildJsonValue(jsonValue map[string]bigquery.JsonValue) (map[string]bigquery.JsonValue, error) {
	jsonValue["__INSERT_ID__"] = fmt.Sprintf("%s-_-%d", m.KeyStr, m.UpdatedAt.UnixNano())
	jsonValue["__INSERT_DATE__"] = time.Now().Unix()
	return jsonValue, nil
}

func (m *Moge) BuildJsonValueWithContext(ctx context.Context, jsonValue map[string]bigquery.JsonValue) (map[string]bigquery.JsonValue, error) {
	jsonValue["__INSERT_ID__"] = fmt.Sprintf("%s-_-%d", m.KeyStr, m.UpdatedAt.UnixNano())
	jsonValue["__INSERT_DATE__"] = time.Now().Unix()

	buf, err := json.Marshal(jsonValue)
	if err != nil {
		log.Errorf(ctx, "Json Value Marshal Error %v", err)
	}
	log.Infof(ctx, "{\"__MOGE_JSON_VALUE__\":%s}", buf)
	return jsonValue, nil
}
