package main

import (
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
	fmt.Println("Analysing Tracing Records from ElasticSearch...")

	// Get Hits
	/*
		spansRecord := createSpanRecordFromJSON("spans.json")
		hits := spansRecord.getHits()
	*/
	hits, _ := getHits("test", 20)

	// Log some data
	hitsCountAsString := strconv.FormatInt(int64(len(hits)), 10)
	log.Println("Hit Count: " + hitsCountAsString)
	for i := 0; i < len(hits); i++ {
		hit := hits[i]
		serviceName := hit.getServiceName()
		operationName := hit.getOperationName()
		startTime := hit.getStartTimeAsLocalTime()
		log.Println("Service/Operation: " + serviceName + "/" + operationName + "(" + startTime + ")" + " ID: " + hit.Id)
	}

	ends := findEndOfTraces("spans.json")
	for i := 0; i < len(ends); i++ {
		end := ends[i]
		start, err := findStartOfTrace("spans.json", end.getTraceId())
		if err != nil {
			log.Println(err.Error())
		}
		log.Println("END: " + timeToStringInSeconds(end.getStartTimeMillis()))
		log.Println("START: " + timeToStringInSeconds(start.getStartTimeMillis()))
		timeDiff := end.getStartTimeMillis() - start.getStartTimeMillis()
		log.Println("TOOK: " + timeToStringInSeconds(timeDiff) + " sec")
		log.Println("TOOK: " + timeToStringInMinutes(timeDiff) + " min")
	}

}

// ElasticSearch
var es_url = "localhost:9200"

// ElasticSearch Helper functions
func getHits(index string, size int) ([]Hit, error) {
	sizeAsString := strconv.FormatInt(int64(size), 10)
	query := "http://" + es_url + "/" + index + "/_search" + "?size=" + sizeAsString
	resp, err := http.Get(query)
	if err != nil {
		log.Println("ERROR: " + err.Error())
		return make([]Hit, 0), err
	}

	var spansRecord SpansRecord
	bodyBytes, _err := ioutil.ReadAll(resp.Body)
	if _err != nil {
		log.Println("ERROR: " + _err.Error())
		return make([]Hit, 0), _err
	}
	json.Unmarshal(bodyBytes, &spansRecord)

	return spansRecord.getHits(), nil
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

// helper
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
