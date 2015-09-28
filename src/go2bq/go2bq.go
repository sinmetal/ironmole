package go2bq

import (
	"fmt"
	"reflect"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

func print(src interface{}) {
	v := reflect.ValueOf(src)
	fmt.Println(fmt.Printf("v.Kind = %s\n", v.Kind()))
	fmt.Println(fmt.Printf("v.NumFields = %d\n", v.Type().NumField()))
	for i := 0; i < v.Type().NumField(); i++ {
		//fmt.Println(v.Type().Field(i))
		fmt.Println(fmt.Printf("i.Type = %s\n", v.Field(i).Type().Name()))
		fmt.Println(fmt.Printf("i.Kind = %s\n", v.Field(i).Kind()))

		for j := 0; j < v.Type().Field(i).Type.NumField(); j++ {
			fmt.Printf("j = %s\n", v.Type().Field(i).Type.Field(j))
			fmt.Printf("j.Name = %s\n", v.Type().Field(i).Type.Field(j).Name)
			fmt.Printf("j.Type = %s\n", v.Type().Field(i).Type.Field(j).Type)
			fmt.Printf("j.Value = %v\n", v.Field(i).Field(j).Interface())
		}
	}
	//fmt.Println(v.Interface())
}

func Print2(body map[string]bigquery.JsonValue, prefix string, src interface{}) {
	v := reflect.ValueOf(src)

	fmt.Println(fmt.Printf("v.Kind = %s\n", v.Kind()))
	fmt.Println(fmt.Printf("v.NumFields = %d\n", v.Type().NumField()))
	for i := 0; i < v.Type().NumField(); i++ {
		if v.Field(i).Kind() == reflect.Struct {
			Print2(body, v.Type().Field(i).Name, v.Field(i).Interface())
		} else {
			fmt.Printf("Name = %s, Value = %v\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Field(i).Interface())
			body[fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name)] = v.Field(i).Interface()
		}
	}
}

func BuildSchema(schema []*bigquery.TableFieldSchema, prefix string, src interface{}) []*bigquery.TableFieldSchema {
	v := reflect.ValueOf(src)

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
				Name: v.Type().Field(i).Name,
				Type: "RECORD",
				Fields: []*bigquery.TableFieldSchema{
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
				},
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
				schemaStruct = BuildSchema(schemaStruct, v.Type().Field(i).Name, v.Field(i).Interface())
				schema = append(schema, &bigquery.TableFieldSchema{
					Name:   v.Type().Field(i).Name,
					Type:   "RECORD",
					Fields: schemaStruct,
				})
			} else {
				fmt.Printf("Name = %s, Value = %v\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Field(i).Interface())
				schema = append(schema, &bigquery.TableFieldSchema{
					Name: v.Type().Field(i).Name,
					Type: func() string {
						switch v.Field(i).Kind() {
						case reflect.Invalid:
						// No-op.
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							return "INTEGER"
						case reflect.Bool:
							return "BOOLEAN"
						case reflect.String:
							return "STRING"
						case reflect.Float32, reflect.Float64:
							return "FLOAT"
						case reflect.Ptr:
							fmt.Println("Ptr = %v", v.Field(i))
							if k, ok := v.Field(i).Interface().(*datastore.Key); ok {
								if k != nil {
									fmt.Println("%v is datastore.Key!", v.Field(i))
								}
							}
						case reflect.Slice:
							fmt.Println("Slice = %v", v.Field(i))
							fmt.Println("Slice Type = %v", reflect.SliceOf(v.Field(i).Type()))

							switch reflect.SliceOf(v.Field(i).Type()) {
							case *datastore.Key:
								fmt.Println("datastore.key = %v", v.Field(i))
							default:
								fmt.Println("default = %v", v.Field(i))
							}
							//							switch slice := v.Field(i).Interface().(type) {
							//								default :
							//								for s := range slice {
							//									fmt.Println(s)
							//								}
							//							}

							//reflect.SliceOf(v.Field(i).Type()).Kind()
						//                        if b, ok := v.Interface().([]byte); ok {
						//                            pv.StringValue = proto.String(string(b))
						//                        } else {
						//                            // nvToProto should already catch slice values.
						//                            // If we get here, we have a slice of slice values.
						//                            unsupported = true
						//                        }
						default:

						}
						return "" // TODO
					}(),
				})
				fmt.Printf("schema = %v\n", schema)
			}
		}
	}
	return schema
}

func BuildJsonValue(jsonValue map[string]bigquery.JsonValue, prefix string, src interface{}) map[string]bigquery.JsonValue {
	v := reflect.ValueOf(src)

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
					name := v.Type().Field(i).Name
					v := map[string]bigquery.JsonValue{
						"namespace": k.Namespace(),
						"app":       k.AppID(),
						"path":      "", // TODO Ancenstor Path
						"kind":      k.Kind(),
						"name":      k.StringID(),
						"id":        k.IntID(),
					}
					jsonValue[name] = v
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
				jsonValueStruct = BuildJsonValue(jsonValueStruct, v.Type().Field(i).Name, v.Field(i).Interface())
				jsonValue[v.Type().Field(i).Name] = jsonValueStruct
			} else {
				fmt.Printf("Name = %s, Value = %v\n", fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name), v.Field(i).Interface())
				jsonValue[v.Type().Field(i).Name] = func() interface{} {
					switch v.Field(i).Kind() {
					case reflect.Invalid:
					// No-op.
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						return v.Field(i).Interface()
					case reflect.Bool:
						return v.Field(i).Interface()
					case reflect.String:
						return v.Field(i).Interface()
					case reflect.Float32, reflect.Float64:
						return v.Field(i).Interface()
					case reflect.Ptr:
						// No-op.
					case reflect.Struct:
						// No-op.
					case reflect.Slice:
						// TODO slice
						fmt.Println("Slice = %v", v.Field(i))
					//                        if b, ok := v.Interface().([]byte); ok {
					//                            pv.StringValue = proto.String(string(b))
					//                        } else {
					//                            // nvToProto should already catch slice values.
					//                            // If we get here, we have a slice of slice values.
					//                            unsupported = true
					//                        }
					default:

					}
					return "" // TODO
				}()
			}
		}
	}
	return jsonValue
}

func bqin() {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 1)
	rows[0] = &bigquery.TableDataInsertAllRequestRows{
		Json: map[string]bigquery.JsonValue{
			"url":         "hoge.com",
			"status_code": 200,
			"start":       time.Now,
			"end":         time.Now,
			"progres_ms":  100,
		},
	}
	fmt.Println("%v", rows[0])
}

func insert(bq *bigquery.Service, url string, statusCode int, start int64, end int64, ms int64) error {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 1)
	rows[0] = &bigquery.TableDataInsertAllRequestRows{
		Json: map[string]bigquery.JsonValue{
			"url":         url,
			"status_code": statusCode,
			"start":       start,
			"end":         end,
			"progres_ms":  ms,
		},
	}
	fmt.Println("%v", rows[0])

	var err error
	for i := 1; i < 10; i++ {
		_, err = bq.Tabledata.InsertAll("cp300demo1", "go2bq", "go2bq_20150905", &bigquery.TableDataInsertAllRequest{
			Kind: "bigquery#tableDataInsertAllRequest",
			Rows: rows,
		}).Do()
		if err != nil {
			fmt.Errorf("%v", err)
			time.Sleep(time.Duration(i) * time.Second)
		} else {
			break
		}
	}
	return err
}

func Insert2(bq *bigquery.Service, projectId string, datasetId string, tableId string, jsonValue map[string]bigquery.JsonValue) (*bigquery.TableDataInsertAllResponse, error) {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 1)
	rows[0] = &bigquery.TableDataInsertAllRequestRows{
		Json: jsonValue,
		//		Json: map[string]bigquery.JsonValue{
		//			"Fuga_Name": "Paaaa",
		//			"Fuga_Age":  0,
		//			"Hoge_Name": "Mogege",
		//			"Hoge_Age":  28,
		//		},
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

func CreateTableMock(bq *bigquery.Service) error {
	t := bigquery.Table{
		TableReference: &bigquery.TableReference{
			ProjectId: "cp300demo1",
			DatasetId: "go2bq",
			TableId:   "go2bq_20150905",
		},
		Schema: &bigquery.TableSchema{
			Fields: []*bigquery.TableFieldSchema{
				{
					Name: "Fuga_Name",
					Type: "STRING",
				},
				{
					Name: "Fuga_Age",
					Type: "INTEGER",
				},
				{
					Name: "Hoge_Name",
					Type: "STRING",
				},
				{
					Name: "Hoge_Age",
					Type: "INTEGER",
				},
				{
					Name: "__key__",
					Type: "RECORD",
					Fields: []*bigquery.TableFieldSchema{
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
					},
				},
			},
		},
	}

	_, err := bq.Tables.Insert("cp300demo1", "go2bq", &t).Do()
	return err
}
