package go2bq

import (
	"encoding/json"
	"fmt"
	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine/datastore"
	"testing"
	"time"
)

func TestBuildSchema(t *testing.T) {
	key := datastore.Key{}

	c := Container2{
		Hoge: Hoge{Name: "hoge", Age: 28},
		Key:  &key,
	}

	schema, err := BuildSchema(&c)
	if err != nil {
		t.Error(err)
	}

	for _, tfs := range schema {
		fmt.Printf("Name : %s, Type : %s \n", tfs.Name, tfs.Type)
	}
}

func TestBuildSchemaMoge(t *testing.T) {
	moge := Moge{}
	schema, err := BuildSchema(&moge)
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

	jsonValue := make(map[string]bigquery.JsonValue)
	_, err := BuildJsonValue(jsonValue, "", moge)
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
