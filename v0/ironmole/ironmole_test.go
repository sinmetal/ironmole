package ironmole

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine/datastore"
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

func (m *Moge) BuildTableSchema(schema []*bigquery.TableFieldSchema) ([]*bigquery.TableFieldSchema, error) {
	return schema, nil
}

func (m *Moge) BuildTableSchemaWithContext(ctx context.Context, schema []*bigquery.TableFieldSchema) ([]*bigquery.TableFieldSchema, error) {
	return schema, nil
}

func (m *Moge) BuildJsonValue(jsonValue map[string]bigquery.JsonValue) (map[string]bigquery.JsonValue, error) {
	return jsonValue, nil
}

func (m *Moge) BuildJsonValueWithContext(ctx context.Context, jsonValue map[string]bigquery.JsonValue) (map[string]bigquery.JsonValue, error) {
	return jsonValue, nil
}

func TestBuildSchema(t *testing.T) {
	key := datastore.Key{}

	c := Container2{
		Hoge: Hoge{Name: "hoge", Age: 28},
		Key:  &key,
	}

	schema, err := BuildTableSchema(&c)
	if err != nil {
		t.Error(err)
	}

	for _, tfs := range schema {
		fmt.Printf("Name : %s, Type : %s \n", tfs.Name, tfs.Type)
	}
}

func TestBuildSchemaMoge(t *testing.T) {
	moge := Moge{}
	schema, err := BuildTableSchema(&moge)
	if err != nil {
		t.Error(err)
	}

	for _, tfs := range schema {
		fmt.Printf("Name : %s, Type : %s \n", tfs.Name, tfs.Type)
		if "RECORD" == tfs.Type {
			for _, field := range tfs.Fields {
				fmt.Printf("Name : %s.%s, Type : %s \n", tfs.Name, field.Name, field.Type)
			}
		}
	}
}

func TestBuildJsonValueMoge(t *testing.T) {
	item := Item{
		Title: "item_title",
	}
	moge := Moge{
		Item:      item,
		Refs:      []string{"momomo", "bababa"},
		CreatedAt: time.Now(),
	}

	jsonValue, err := BuildJsonValue(&moge)
	if err != nil {
		t.Errorf("BuildJsonValue error %v", err)
	}

	buf, err := json.Marshal(jsonValue)
	if err != nil {
		t.Errorf("Json Value Marshal Error %v", err)
	}
	t.Logf("JsonValue : %s", buf)

	fmt.Println(jsonValue)
}

func TestTableSchemaBuilderImplements(t *testing.T) {
	moge := &Moge{}

	var src interface{}
	src = moge

	_, ok := src.(TableSchemaBuilder)
	if ok == false {
		t.Errorf("moge is not implements TableSchemaBuilder")
	}
}

func TestJsonValueWithContextBuilderImplements(t *testing.T) {
	moge := &Moge{}

	var src interface{}
	src = moge

	_, ok := src.(JsonValueWithContextBuilder)
	if ok == false {
		t.Errorf("moge is not implements JsonValueWithContextBuilder")
	}
}
