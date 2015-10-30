package go2bq

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"golang.org/x/net/context"

	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

type TableSchemaBuilder interface {
	BuildTableSchema(schema []*bigquery.TableFieldSchema) ([]*bigquery.TableFieldSchema, error)
}

type TableSchemaWithContextBuilder interface {
	BuildTableSchemaWithContext(ctx context.Context, schema []*bigquery.TableFieldSchema) ([]*bigquery.TableFieldSchema, error)
}

type JsonValueBuilder interface {
	BuildJsonValue(jsonValue map[string]bigquery.JsonValue) (map[string]bigquery.JsonValue, error)
}

type JsonValueWithContextBuilder interface {
	BuildJsonValueWithContext(ctx context.Context, jsonValue map[string]bigquery.JsonValue) (map[string]bigquery.JsonValue, error)
}

func BuildTableSchema(src interface{}) ([]*bigquery.TableFieldSchema, error) {
	schema := make([]*bigquery.TableFieldSchema, 0, 10)
	schema, err := buildTableSchema(schema, "", src)
	if err != nil {
		return schema, err
	}
	if e, ok := src.(TableSchemaBuilder); ok {
		schema, err = e.BuildTableSchema(schema)
	}
	return schema, err
}

func BuildTableSchemaWithContext(ctx context.Context, src interface{}) ([]*bigquery.TableFieldSchema, error) {
	schema := make([]*bigquery.TableFieldSchema, 0, 10)
	schema, err := buildTableSchema(schema, "", src)
	if err != nil {
		return schema, err
	}
	if e, ok := src.(TableSchemaWithContextBuilder); ok {
		schema, err = e.BuildTableSchemaWithContext(ctx, schema)
	}
	return schema, err
}

func buildTableSchema(schema []*bigquery.TableFieldSchema, prefix string, src interface{}) ([]*bigquery.TableFieldSchema, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Interface && !v.IsNil() {
		elm := v.Elem()
		if elm.Kind() == reflect.Ptr && !elm.IsNil() && elm.Elem().Kind() == reflect.Ptr {
			v = elm
		}
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	fmt.Println(fmt.Printf("v.Kind = %s\n", v.Kind()))
	fmt.Println(fmt.Printf("v.NumFields = %d\n", v.Type().NumField()))
	for i := 0; i < v.Type().NumField(); i++ {
		if len(v.Type().Field(i).PkgPath) > 0 {
			fmt.Printf("%s is Unexported\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name))
			continue
		}
		fmt.Printf("%s run start, PkgPath = %s\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Type().Field(i).PkgPath)

		switch x := v.Field(i).Interface().(type) {
		case *datastore.Key:
			fmt.Printf("%s is datastore.Key!, PkgPath = %s, x = %v\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Type().Field(i).PkgPath, x)
			schema = append(schema, &bigquery.TableFieldSchema{
				Name:   v.Type().Field(i).Name,
				Type:   "RECORD",
				Fields: createKeySchema(),
			})
		case time.Time:
			schema = append(schema, &bigquery.TableFieldSchema{
				Name: v.Type().Field(i).Name,
				Type: "TIMESTAMP",
			})
		case appengine.BlobKey:
		//p.Value = x
		case appengine.GeoPoint:
		//p.Value = x
		case datastore.ByteString:
			// byte列はスルー
		default:
			fmt.Printf("x is default, %v\n", x)

			if v.Field(i).Kind() == reflect.Struct {
				schemaStruct := make([]*bigquery.TableFieldSchema, 0, 10)
				schemaStruct, err := buildTableSchema(schemaStruct, v.Type().Field(i).Name, v.Field(i).Interface())
				if err != nil {
					return schemaStruct, nil
				}
				schema = append(schema, &bigquery.TableFieldSchema{
					Name:   v.Type().Field(i).Name,
					Type:   "RECORD",
					Fields: schemaStruct,
				})
			} else {
				fmt.Printf("Name = %s, Value = %v\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Field(i).Interface())
				tfs := func() *bigquery.TableFieldSchema {
					switch v.Field(i).Kind() {
					case reflect.Invalid:
						// No-op.
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						return &bigquery.TableFieldSchema{
							Name: v.Type().Field(i).Name,
							Type: "INTEGER"}
					case reflect.Bool:
						return &bigquery.TableFieldSchema{
							Name: v.Type().Field(i).Name,
							Type: "BOOLEAN"}
					case reflect.String:
						return &bigquery.TableFieldSchema{
							Name: v.Type().Field(i).Name,
							Type: "STRING"}
					case reflect.Float32, reflect.Float64:
						return &bigquery.TableFieldSchema{
							Name: v.Type().Field(i).Name,
							Type: "FLOAT"}
					case reflect.Ptr:
						fmt.Println("Ptr = %v", v.Field(i))
						if k, ok := v.Field(i).Interface().(*datastore.Key); ok {
							if k != nil {
								fmt.Println("%v is datastore.Key!", v.Field(i))
							}
						}
					case reflect.Slice:
						fmt.Println("Slice = %v", v.Field(i))
						fmt.Println(v.Field(i).Type().Elem())
						fmt.Println(v.Field(i).Type().Elem().Kind())

						elem := v.Field(i).Type().Elem()
						switch elem {
						case reflect.TypeOf(&datastore.Key{}):
							return &bigquery.TableFieldSchema{
								Name:   v.Type().Field(i).Name,
								Type:   "RECORD",
								Fields: createKeySchema(),
								Mode:   "REPEATED"}
						default:
							fmt.Println("slice default")
							switch elem.Kind() {
							case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
								return &bigquery.TableFieldSchema{
									Name: v.Type().Field(i).Name,
									Type: "INTEGER",
									Mode: "REPEATED"}
							case reflect.Bool:
								return &bigquery.TableFieldSchema{
									Name: v.Type().Field(i).Name,
									Type: "BOOLEAN",
									Mode: "REPEATED"}
							case reflect.String:
								return &bigquery.TableFieldSchema{
									Name: v.Type().Field(i).Name,
									Type: "STRING",
									Mode: "REPEATED"}
							case reflect.Float32, reflect.Float64:
								return &bigquery.TableFieldSchema{
									Name: v.Type().Field(i).Name,
									Type: "FLOAT",
									Mode: "REPEATED"}
							default:
								fmt.Println("slice default") // TODO
							}
						}

						return nil // TODO
					default:

					}
					return nil // TODO
				}()
				schema = append(schema, tfs)
				fmt.Printf("schema = %v\n", schema)
			}
		}
	}
	return schema, nil
}

func BuildJsonValue(src interface{}) (map[string]bigquery.JsonValue, error) {
	jsonValue := make(map[string]bigquery.JsonValue)

	jsonValue, err := buildJsonValueInternal(jsonValue, "", src)
	if err != nil {
		return jsonValue, err
	}
	if e, ok := src.(JsonValueBuilder); ok {
		jsonValue, err = e.BuildJsonValue(jsonValue)
	}
	return jsonValue, err
}

func BuildJsonValueWithContext(ctx context.Context, src interface{}) (map[string]bigquery.JsonValue, error) {
	jsonValue := make(map[string]bigquery.JsonValue)

	jsonValue, err := buildJsonValueInternal(jsonValue, "", src)
	if err != nil {
		return jsonValue, err
	}
	if e, ok := src.(JsonValueWithContextBuilder); ok {
		jsonValue, err = e.BuildJsonValueWithContext(ctx, jsonValue)
	}
	return jsonValue, err
}

func buildJsonValueInternal(jsonValue map[string]bigquery.JsonValue, prefix string, src interface{}) (map[string]bigquery.JsonValue, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Interface && !v.IsNil() {
		elm := v.Elem()
		if elm.Kind() == reflect.Ptr && !elm.IsNil() && elm.Elem().Kind() == reflect.Ptr {
			v = elm
		}
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	fmt.Println(fmt.Printf("v.Kind = %s\n", v.Kind()))
	fmt.Println(fmt.Printf("v.NumFields = %d\n", v.Type().NumField()))
	for i := 0; i < v.Type().NumField(); i++ {
		if len(v.Type().Field(i).PkgPath) > 0 {
			fmt.Printf("%s is Unexported\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name))
			continue
		}
		fmt.Printf("%s run start, PkgPath = %s\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Type().Field(i).PkgPath)

		switch x := v.Field(i).Interface().(type) {
		case *datastore.Key:
			fmt.Printf("%s is datastore.Key!, PkgPath = %s, x = %v\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Type().Field(i).PkgPath, x)
			if k, ok := v.Field(i).Interface().(*datastore.Key); ok {
				if k != nil {
					var err error
					jsonValue[v.Type().Field(i).Name], err = buildDatastoreKey(k)
					if err != nil {
						return nil, err
					}
				}
			}

		case time.Time:
			jsonValue[v.Type().Field(i).Name] = x.Unix()
		case appengine.BlobKey:
		//p.Value = x
		case appengine.GeoPoint:
		//p.Value = x
		case datastore.ByteString:
		// byte列はスルー
		default:
			fmt.Printf("x is default, %v\n", x)

			if v.Field(i).Kind() == reflect.Struct {
				jsonValueStruct := make(map[string]bigquery.JsonValue)
				jsonValueStruct, err := buildJsonValueInternal(jsonValueStruct, v.Type().Field(i).Name, v.Field(i).Interface())
				if err != nil {
					return nil, err
				}
				jsonValue[v.Type().Field(i).Name] = jsonValueStruct
			} else {
				fmt.Printf("Name = %s, Value = %v\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Field(i).Interface())
				value, err := func() (interface{}, error) {
					switch v.Field(i).Kind() {
					case reflect.Invalid:
					// No-op.
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						return v.Field(i).Interface(), nil
					case reflect.Bool:
						return v.Field(i).Interface(), nil
					case reflect.String:
						return v.Field(i).Interface(), nil
					case reflect.Float32, reflect.Float64:
						return v.Field(i).Interface(), nil
					case reflect.Ptr:
						// No-op.
					case reflect.Struct:
						// No-op.
					case reflect.Slice:
						fmt.Println("Slice = %v", v.Field(i))
						l := v.Field(i).Len()
						jv := make([]interface{}, l)
						for j := 0; j < l; j++ {
							elemV := v.Field(i).Index(j).Interface()
							if ev, ok := elemV.(*datastore.Key); ok {
								var err error
								jv[j], err = buildDatastoreKey(ev)
								if err != nil {
									return nil, err
								}
							} else {
								switch v.Field(i).Type().Elem().Kind() {
								case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
									jv[j] = elemV
								case reflect.Bool:
									jv[j] = elemV
								case reflect.String:
									fmt.Println("slice loop string")
									jv[j] = elemV
								case reflect.Float32, reflect.Float64:
									jv[j] = elemV
								default:
									fmt.Println("slice default")
								}
							}
						}
						return jv, nil
					default:

					}
					return "", nil // TODO
				}()
				if err != nil {
					return nil, err
				}
				jsonValue[v.Type().Field(i).Name] = value
			}
		}
	}
	return jsonValue, nil
}

func Insert(bq *bigquery.Service, projectId string, datasetId string, tableId string, jsonValue map[string]bigquery.JsonValue) (*bigquery.TableDataInsertAllResponse, error) {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 1)
	rows[0] = &bigquery.TableDataInsertAllRequestRows{
		Json: jsonValue,
	}
	fmt.Println("%v", rows[0])

	return bq.Tabledata.InsertAll(projectId, datasetId, tableId, &bigquery.TableDataInsertAllRequest{
		Kind: "bigquery#tableDataInsertAllRequest",
		Rows: rows,
	}).Do()
}

func CreateTable(bq *bigquery.Service, projectId string, datasetId string, tableId string, schema []*bigquery.TableFieldSchema) error {
	t := bigquery.Table{
		TableReference: &bigquery.TableReference{
			TableId: tableId,
		},
		Schema: &bigquery.TableSchema{
			Fields: schema,
		},
	}

	_, err := bq.Tables.Insert(projectId, datasetId, &t).Do()
	return err
}

func createKeySchema() []*bigquery.TableFieldSchema {
	return []*bigquery.TableFieldSchema{
		{
			Name: "namespace",
			Type: "STRING",
		},
		{
			Name: "app",
			Type: "STRING",
		},
		{
			Name: "path",
			Type: "STRING",
		},
		{
			Name: "kind",
			Type: "STRING",
		},
		{
			Name: "name",
			Type: "STRING",
		},
		{
			Name: "id",
			Type: "INTEGER",
		},
	}
}

func buildDatastoreKey(key *datastore.Key) (map[string]bigquery.JsonValue, error) {
	if key == nil {
		return map[string]bigquery.JsonValue{
			"namespace": "",
			"app":       "",
			"path":      "",
			"kind":      "",
			"name":      "",
			"id":        0,
		}, nil
	}

	var workKey = key
	var keys []*datastore.Key
	keys = append(keys, key)
	for {
		if workKey.Parent() == nil {
			break
		}
		keys = append(keys, workKey.Parent())
		workKey = workKey.Parent()
	}

	var buf bytes.Buffer
	for i := len(keys) - 1; i >= 0; i-- {
		if buf.Len() > 0 {
			_, err := buf.WriteString(", ")
			if err != nil {
				return map[string]bigquery.JsonValue{}, nil
			}
		}

		key := keys[i]
		if len(key.StringID()) < 1 {
			_, err := buf.WriteString(fmt.Sprintf(`"%s", "%s"`, keys[i].Kind(), keys[i].IntID()))
			if err != nil {
				return map[string]bigquery.JsonValue{}, nil
			}
		} else {
			_, err := buf.WriteString(fmt.Sprintf(`"%s", "%s"`, keys[i].Kind(), keys[i].StringID()))
			if err != nil {
				return map[string]bigquery.JsonValue{}, nil
			}
		}
	}

	return map[string]bigquery.JsonValue{
		"namespace": key.Namespace(),
		"app":       key.AppID(),
		"path":      buf.String(),
		"kind":      key.Kind(),
		"name":      key.StringID(),
		"id":        key.IntID(),
	}, nil
}
