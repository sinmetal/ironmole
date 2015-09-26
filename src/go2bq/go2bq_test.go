package go2bq

import (
	"fmt"
	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine/datastore"
	"testing"
)

func TestPrint(t *testing.T) {
	c := Container{
		Hoge: Hoge{Name: "hoge", Age: 28},
	}
	print(c)
}

func TestPrint2(t *testing.T) {
	c := Container{
		Hoge: Hoge{Name: "hoge", Age: 28},
	}
	body := map[string]bigquery.JsonValue{}
	Print2(body, "", c)

	fmt.Printf("%v", body)
}

func TestBuildSchema(t *testing.T) {
	key := datastore.Key{}

	c := Container2{
		Hoge: Hoge{Name: "hoge", Age: 28},
		Key:  &key,
	}
	schema := make([]*bigquery.TableFieldSchema, 0, 10)
	schema = BuildSchema(schema, "", c)

	for _, tfs := range schema {
		fmt.Printf("Name : %s, Type : %s \n", tfs.Name, tfs.Type)
	}
}

func TestBQIn(t *testing.T) {
	bqin()
}
