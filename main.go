package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/mottaquikarim/esquerydsl"
)

const (
	CLOUD_ID = "stackdeployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQyMGQ1YTQ3NmFmOWI0ZTI0ODFiZGZlOGRlMWM5YzdlZiQ2MDFkODdkMWM0MTU0NmQ0YTY3MGM5NDU4N2Q3MWVlYg=="
	API_KEY  = "eFVEOVhJRUJGYWR0VWE1WVJ3QXo6YjVGNzczYy1SaHFmMWxqaEFEYjBVdw=="
)

type Case struct {
	CaseID int    `json:"case_id"`
	Query  string `json:"query"`
}

var ResultMap map[int]*esapi.Response

type Doc struct {
	Title string `json:"title"`
}

func init() {
	log.SetFlags(0)
}

func main() {
	var (
		// res *esapi.Response
		err error
	)
	ResultMap = make(map[int]*esapi.Response, 0)

	cfg := elasticsearch.Config{
		CloudID: CLOUD_ID,
		APIKey:  API_KEY,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		fmt.Println(err)
	}

	cases, err := ReadCases("examples/cases.json")
	if err != nil {
		fmt.Println(err)
	}

	jsonFile, err := os.Open("examples/messages copy.json")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened json file")
	defer jsonFile.Close()

	scanner := bufio.NewScanner(jsonFile)

	// Default scanner is bufio.ScanLines.
	// Could also use a custom function of SplitFunc type
	scanner.Split(bufio.ScanLines)
	msgSlice := make([]string, 0)

	// Scan for next token
	for scanner.Scan() {
		if scanner.Text() != "" {

			msgSlice = append(msgSlice, scanner.Text())
		}
	}
	msgSlice = RemoveIndex(msgSlice, 0)
	msgSlice = RemoveIndex(msgSlice, len(msgSlice)-1)

	docSlice := make([]Doc, 0)

	for _, str := range msgSlice {
		docSlice = append(docSlice, Doc{Title: str})
	}

	for _, doc := range docSlice {
		_, err = es.Index("test", esutil.NewJSONReader(&doc), es.Index.WithRefresh("true"))
		if err != nil {
			log.Fatalf("Error getting response: %s", err)
		}

		// log.Println(res)
	}

	for _, oneCase := range cases {
		numberANDs := splitAND(oneCase.Query)
		if len(numberANDs) > 1 {
			and := []esquerydsl.QueryItem{}
			or := []esquerydsl.QueryItem{}
			for _, numberAND := range numberANDs {

				switch length := len(splitOR(numberAND)); {
				case length == 1:
					qi := esquerydsl.QueryItem{
						Field: "title",
						Value: numberAND,
						Type:  esquerydsl.Match,
					}
					and = append(and, qi)

					fallthrough

				case length > 1:
					qi := esquerydsl.QueryItem{
						Field: "title",
						Value: numberAND,
						Type:  esquerydsl.Match,
					}
					and = append(and, qi)

					SearchQuery(and, or, oneCase.CaseID, es)

				default:
					fmt.Println("Good night!")
				}
			}

		} else {
			numberORs := splitOR(numberANDs[0])
			if len(numberORs) > 1 {
				and := []esquerydsl.QueryItem{}
				or := []esquerydsl.QueryItem{}
				for _, numberOR := range numberORs {
					qi := esquerydsl.QueryItem{
						Field: "title",
						Value: numberOR,
						Type:  esquerydsl.Match,
					}
					or = append(or, qi)
				}
				SearchQuery(and, or, oneCase.CaseID, es)

			} else {

				and := []esquerydsl.QueryItem{
					{
						Field: "title",
						Value: numberORs[0],
						Type:  esquerydsl.Match,
					}}
				or := []esquerydsl.QueryItem{}
				SearchQuery(and, or, oneCase.CaseID, es)
			}

		}

	}
	log.Println(ResultMap)

}

func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

func splitAND(s string) []string {
	return strings.Split(s, "AND")
}

func splitOR(s string) []string {
	return strings.Split(s, "OR")
}

func ReadCases(filename string) ([]Case, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var cases []Case
	if err := json.Unmarshal(b, &cases); err != nil {
		return []Case{}, nil
	}

	return cases, nil
}

func SearchQuery(and, or []esquerydsl.QueryItem, caseID int, es *elasticsearch.Client) map[int]*esapi.Response {
	bodyQuery, _ := json.Marshal(esquerydsl.QueryDoc{
		Index: "test",

		And: and,
		Or:  or,
	})

	// Build a new string from JSON query
	var b strings.Builder
	b.WriteString(string(bodyQuery))

	// Instantiate a *strings.Reader object from string
	read := strings.NewReader(b.String())

	res1, err := es.Search(
		es.Search.WithIndex("test"),
		es.Search.WithBody(read),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	ResultMap[caseID] = res1
	// log.Println(ResultMap)
	return ResultMap

}
