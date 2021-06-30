package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

/*
Duration from CTLE - XML (Change is written to Feed)
START: "process-ctle" (Service: collector-mercado-worker)
END: "write-to-mongo" (Service: changeprocessor)
*/

func main() {

	// Constants
	// oneDayInMilliSeconds := 86400000

	// START: collector-mercado-worker-v1

	fmt.Println("Analysing Tracing Records from ElasticSearch...")

	// Get chunks of 1 hour from 0-24 h
	tc := todayChunks()
	lenAsString := strconv.FormatInt(int64(len(tc)), 10)
	fmt.Println("TC: " + lenAsString)

	index := indexToday()

	for i := 0; i < 24; i++ {
		from := strconv.FormatInt(int64(tc[i]), 10)
		to := strconv.FormatInt(int64(tc[i+1]-1), 10) // -1 => otherwise it will be found twice
		hits, _ := query(index, "changeprocessor", "write-to-mongo", from, to, "desc", 1000)
		hitsCountAsString := strconv.FormatInt(int64(len(hits)), 10)

		indexAsString := strconv.FormatInt(int64(i), 10)
		log.Println("Index " + indexAsString + "-> Hit Count: " + hitsCountAsString)

		fmt.Println(" --- FROM: " + timeStampInMilliSecondsToLocal(tc[i]) + "- TO:" + timeStampInMilliSecondsToLocal(tc[i+1]) + "---")
		for i := 0; i < len(hits); i++ {
			end := hits[i]
			start, err := queryStart(index, "collector-mercado-worker", "process-ctle", end.getTraceId())
			if err != nil {
				log.Println("ERROR: " + err.Error())
			}
			// endTime := timeToStringInSeconds(end.getStartTimeMillis())
			// startTime := timeToStringInSeconds(start.getStartTimeMillis())
			timeDiff := end.getStartTimeMillis() - start.getStartTimeMillis()
			// log.Println("END: " + endTime + " ,START: " + startTime)
			log.Println("TOOK: " + timeToStringInSeconds(timeDiff) + " sec " + "(" + timeToStringInMinutes(timeDiff) + " min" + ")")
		}
	}

	/*
		hits, _ := queryAll("test")
		hitsCountAsString := strconv.FormatInt(int64(len(hits)), 10)
		log.Println("Hits found: " + hitsCountAsString)
		for _, hit := range hits {
			log.Println("HIT: " + hit.getOperationName() + ", Service: " + hit.getServiceName() + "(" + hit.getStartTimeAsLocalTime() + ")")
		}
	*/
}

// ElasticSearch
// var es_url = "http://localhost:9200" // via ubuntu server elastic search installation (login needed)

func getElasticSearchURL() string {
	var es_url = os.Getenv("ES_DEV_JAEGER")
	// fmt.Println("ES_URL: " + es_url)

	es_url = "http://localhost:9200"
	return es_url
}

// ElasticSearch Helper functions
func queryAll(index string) ([]Hit, error) {

	query := getElasticSearchURL() + "/" + index + "/_search?size=10000"

	req, err := http.NewRequest("GET", query, nil)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var spansRecord SpansRecord
	bodyBytes, _err := ioutil.ReadAll(resp.Body)
	if _err != nil {
		log.Println("ERROR: " + _err.Error())
		return make([]Hit, 0), _err
	}
	json.Unmarshal(bodyBytes, &spansRecord)

	return spansRecord.getHits(), nil
}

func query(index string, serviceName string, operationName string, from string, to string, sort string, size int) ([]Hit, error) {

	query := getElasticSearchURL() + "/" + index + "/_search"

	bodyString := createBodyWithRange(serviceName, operationName, from, to, sort, size)
	var body = []byte(bodyString)

	req, err := http.NewRequest("GET", query, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var spansRecord SpansRecord
	bodyBytes, _err := ioutil.ReadAll(resp.Body)
	if _err != nil {
		log.Println("ERROR: " + _err.Error())
		return make([]Hit, 0), _err
	}
	json.Unmarshal(bodyBytes, &spansRecord)

	return spansRecord.getHits(), nil
}

func queryStart(index string, serviceName string, operationName string, traceID string) (Hit, error) {

	query := getElasticSearchURL() + "/" + index + "/_search"

	bodyString := createBody(serviceName, operationName, traceID)
	var body = []byte(bodyString)

	req, err := http.NewRequest("GET", query, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var spansRecord SpansRecord
	bodyBytes, _err := ioutil.ReadAll(resp.Body)
	if _err != nil {
		log.Println("ERROR: " + _err.Error())
		return Hit{}, _err
	}
	json.Unmarshal(bodyBytes, &spansRecord)

	hits := spansRecord.getHits()
	if len(hits) != 1 {
		log.Println("ERROR: No Start found for TraceID: " + traceID)
		return Hit{}, err
	} else {
		return hits[0], nil
	}
}

func findEndOfTraces(path string) []Hit {
	spansRecord := createSpanRecordFromJSON("spans.json")
	hits := spansRecord.getHits()
	ends := make([]Hit, 0)
	for i := 0; i < len(hits); i++ {
		hit := hits[i]
		if hit.getOperationName() == "write-to-mongo" {
			ends = append(ends, hit)
		}
	}
	return ends
}

///////////
// helper//
///////////

// Elastic Search Index for Today
func indexToday() string {
	now := time.Now()
	yearAsString := strconv.Itoa(now.Year())
	var monthAsString string
	if now.Month() < 10 {
		monthAsString = "0" + strconv.Itoa(int(now.Month()))
	} else {
		monthAsString = strconv.Itoa(int(now.Month()))
	}
	dayAsString := strconv.Itoa(now.Day())
	indexToday := "jaeger-span-" + yearAsString + "-" + monthAsString + "-" + dayAsString
	// fmt.Println("INDEX: " + indexToday)

	// indexToday = "test"
	return indexToday
}

func todayMidnightUnixTimeInMilliSeconds() int64 {
	now := time.Now()
	year := now.Year()
	month := now.Month()
	day := now.Day()

	midnight := time.Date(year, month, day, 0, 0, 0, 0, now.Location())
	loc, _ := time.LoadLocation("Europe/Berlin")
	fmt.Println("TimeStamp Midnight: " + midnight.In(loc).String())

	ut := midnight.UnixNano() / int64(time.Millisecond)
	utAsString := strconv.FormatInt(int64(ut), 10)
	fmt.Println("TimeStamp in Unix (Millis): " + utAsString)
	return ut
}

// 25 entries to cover 23-24h also
func todayChunks() []int64 {
	var chunks = make([]int64, 0)
	midnight := todayMidnightUnixTimeInMilliSeconds()
	// midnight = timeStampInMilliSeconds("2021-06-23T00:00:00+02:00") // local testing
	oneHourInMilliSeconds := 3600000

	for hours := 0; hours < 25; hours++ {
		chunks = append(chunks, int64(hours)*int64(oneHourInMilliSeconds)+midnight)
	}

	/*
		for i, chunk := range chunks {
			index := strconv.FormatInt(int64(i), 10)
			fmt.Println(index + "-CHUNK: " + strconv.FormatInt(int64(chunk), 10))
		}
	*/

	return chunks
}

// Body Builder for GET Requests
func createBodyWithRange(serviceName string, operationName string, from string, to string, sort string, size int) string {
	sizeAsString := strconv.FormatInt(int64(size), 10)

	if size > 10000 {
		log.Println("[WARN]: Size is bigger than 10,000 - size will be set to 10,000")
		sizeAsString = "10000"
	}

	var jsonString = `{
		"from": 0,
		"size": ` + sizeAsString + `,
		"query": {
			"bool": {
				"must": [
					{
						"match_phrase": {
							"process.serviceName": "` + serviceName + `"
						}
					},
					{
						"match_phrase": {
							"operationName": "` + operationName + `"
						}
					},
					{
						"range": {
							"startTimeMillis": {
								"gte": ` + from + `,
								"lte": ` + to + `
							}
						}
					}
				]
			}
		},
		"sort": [
			{
				"startTimeMillis": "` + sort + `"
			}
		]
	}`
	//fmt.Println("BODY: " + bodyString)
	return jsonString
}

// Find Start Point
func createBody(serviceName string, operationName string, traceID string) string {

	var jsonString = `{
		"from": 0,
		"size": 10,
		"query": {
			"bool": {
				"must": [
					{
						"match_phrase": {
							"process.serviceName": "` + serviceName + `"
						}
					},
					{
						"match_phrase": {
							"operationName": "` + operationName + `"
						}
					},
					{
						"match_phrase": {
							"traceID": "` + traceID + `"
						}
					}
				]
			}
		}
	}`
	//fmt.Println("BODY: " + bodyString)
	return jsonString
}

// RFC3339 timestamp in milliseconds (unix time) - e.g: "2021-06-14T09:58:16+02:00" => 1623657496000
func timeStampInMilliSeconds(rfc3339t string) int64 {

	t, err := time.Parse(time.RFC3339, rfc3339t)
	if err != nil {
		panic(err)
	}

	// convert into unix time
	loc, _ := time.LoadLocation("Europe/Berlin")
	fmt.Println("TimeStamp: " + t.In(loc).String())

	ut := t.UnixNano() / int64(time.Millisecond)
	//utAsString := strconv.FormatInt(int64(ut), 10)
	//return utAsString

	return ut
}

func timeStampInMilliSecondsToLocal(timeStampInMillis int64) string {

	timeStampSecs := timeStampInMillis / 1000
	tm := time.Unix(timeStampSecs, 0)
	loc, _ := time.LoadLocation("Europe/Berlin")
	return tm.In(loc).String()
}

func timeToStringInSeconds(milliSecs int64) string {
	secs := milliSecs / 1000
	numberAsString := strconv.FormatInt(int64(secs), 10)
	return numberAsString
}

func timeToStringInMinutes(milliSecs int64) string {
	float := float64(milliSecs)
	minutes := float / 1000.0 / 60.0
	numberAsString := strconv.FormatFloat(minutes, 'f', 2, 64)
	return numberAsString
}

// errorString is a trivial implementation of error.
type errorString struct {
	s string
}

func (e *errorString) Error() string {
	return e.s
}

func findStartOfTrace(path string, traceID string) (Hit, error) {
	spansRecord := createSpanRecordFromJSON("spans.json")
	var startHit Hit
	hits := spansRecord.getHits()
	starts := make([]Hit, 0)
	for i := 0; i < len(hits); i++ {
		hit := hits[i]
		if hit.getTraceId() == traceID && hit.getOperationName() == "process-ctle" {
			starts = append(starts, hit)
		}
	}
	if len(starts) != 1 {
		lenghtAsString := strconv.FormatInt(int64(len(starts)), 10)
		return startHit, &errorString{"Wrong number Start-Traces found: " + lenghtAsString + ", instead of 1"}
	} else {
		startHit := starts[0]
		return startHit, nil
	}
}

func (spansRecord *SpansRecord) getHits() []Hit {
	return spansRecord.Hits.Hits
}

func (hit *Hit) getTraceId() string {
	return hit.Source.TraceID
}

func (hit *Hit) getSpanId() string {
	return hit.Source.SpanID
}

func (hit *Hit) getStartTimeMillis() int64 {
	return hit.Source.StartTimeMillis
}

func (hit *Hit) getStartTimeAsLocalTime() string {
	timeStampSecs := hit.Source.StartTimeMillis / 1000
	tm := time.Unix(timeStampSecs, 0)
	loc, _ := time.LoadLocation("Europe/Berlin")
	return tm.In(loc).String()
}

func (hit *Hit) getOperationName() string {
	return hit.Source.OperationName
}

func (hit *Hit) getServiceName() string {
	return hit.Source.Process.ServiceName
}

// **************************************************************** //
// SpansRecord - Tracing Spans (Jaeger), curled from ElasticSearch	//
// **************************************************************** //

// SpansRecord - NOT nested
type SpansRecord struct {
	Took     int         `json:"took"`
	TimedOut bool        `json:"timed_out"`
	Shards   Shards      `json:"_shards"`
	Hits     HitsSummery `json:"hits"`
}

type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type HitsSummery struct {
	Total    Total   `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []Hit   `json:"hits"`
}

type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type Hit struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Id     string  `json:"_id"`
	Score  float64 `json:"_score"`
	Source Source  `json:"_source"`
}

// Source - Logs are not defined yet
type Source struct {
	TraceID         string        `json:"traceID"`
	SpanID          string        `json:"spanID"`
	Flags           int           `json:"flags"`
	OperationName   string        `json:"operationName"`
	References      []interface{} `json:"references"`
	StartTime       int64         `json:"startTime"`
	StartTimeMillis int64         `json:"startTimeMillis"`
	Duration        int           `json:"duration"`
	Tags            []Tag         `json:"tags"`
	Logs            []interface {
	} `json:"logs"`
	Process Process `json:"process"`
}

type Process struct {
	ServiceName string `json:"serviceName"`
	Tags        []Tag  `json:"tags"`
}

type Tag struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// For Local testing only
func createSpanRecordFromJSON(path string) SpansRecord {

	var spansRecord SpansRecord

	jsonFile, err := os.Open(path)
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened: " + path)

	// read our opened jsonFile as a byte array and unmarshal it (convert JSON into Struct)
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &spansRecord)

	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	return spansRecord
}
