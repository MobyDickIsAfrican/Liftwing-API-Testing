package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

var (
	RevisionLimit = 50
)

type Revision struct {
	ID    int    `json:"id"`
	NS    int    `json:"ns"`
	Title string `json:"title"`
}

type Count struct {
	mu    sync.Mutex
	count int
}

type APIResponse struct {
	BatchComplete string `json:"batchcomplete"`
	Continue      struct {
		RNContinue string `json:"rncontinue"`
		Continue   string `json:"continue"`
	} `json:"continue"`
	Query struct {
		Random []struct {
			ID    int    `json:"id"`
			NS    int    `json:"ns"`
			Title string `json:"title"`
		} `json:"random"`
	} `json:"query"`
}

type Articles struct {
	Response          *APIResponse
	ProjectIdentifier string
}

func (c *Count) Increment() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}

func (c *Count) GetCount() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return float64(c.count)
}

func main() {
	var randomArticles = make(chan Articles, RevisionLimit)
	var wg sync.WaitGroup

	db, err := sql.Open("sqlite3", "./response_times.db")
	if err != nil {
		log.Fatal(err)
	}

	err = godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}
	defer db.Close()
	statement, _ := db.Prepare("CREATE TABLE IF NOT EXISTS response_times (revision_id INTEGER, project_id TEXT, response_time REAL)")
	statement.Exec()

	wg.Add(1)
	go func() {
		wikiProjects := map[int]string{
			1:  "en",
			2:  "de",
			3:  "fr",
			4:  "it",
			5:  "es",
			6:  "ru",
			7:  "ja",
			8:  "nl",
			9:  "pt",
			10: "pl",
			11: "zh",
			12: "sv",
			13: "vi",
			14: "uk",
			15: "ca",
			16: "no",
			17: "fi",
			18: "hu",
			19: "ko",
			20: "id",
		}
		for {
			rand.Seed(time.Now().UnixNano())
			randomIndex := rand.Intn(20) + 1
			project := wikiProjects[randomIndex]

			query := fmt.Sprintf("https://%s.wikipedia.org/w/api.php?action=query&list=random&rnnamespace=0&rnlimit=%d&format=json", project, RevisionLimit)
			req, _ := http.NewRequest("GET", query, nil)
			resp, err := http.DefaultClient.Do(req)

			if err != nil {
				log.Print(err)
				continue
			}
			body, _ := ioutil.ReadAll(resp.Body)

			var apiRes APIResponse
			json.Unmarshal(body, &apiRes)
			articles := Articles{Response: &apiRes, ProjectIdentifier: project}

			randomArticles <- articles
		}
	}()

	count := &Count{}

	t0 := time.Now()
	dur := getDuration()
	for rat := range randomArticles {
		t1 := time.Since(t0).Seconds()
		if t1 > float64(dur)*60 {
			break
		}

		c := count.GetCount()
		slp := calculateSleep(c)

		for _, rev := range rat.Response.Query.Random {
			wg.Add(1)
			go Worker(rat.ProjectIdentifier, rev, count, &wg, slp, db)
		}
	}

	var length int
	dbErr := db.QueryRow("SELECT COUNT(*) FROM response_times").Scan(&length)
	if dbErr != nil {
		log.Fatal(dbErr)
	}

	fmt.Printf("Total requests: %d\n", length)

	var responseCount int
	row := db.QueryRow("SELECT COUNT(*) FROM response_times WHERE response_time <= 0.2")
	row.Scan(&responseCount)
	fmt.Printf("200ms and under: %.2f%%\n", float64(responseCount)/float64(length))

	row = db.QueryRow("SELECT COUNT(*) FROM response_times WHERE response_time <= 0.3")
	row.Scan(&responseCount)
	fmt.Printf("300ms and under: %.2f%%\n", float64(responseCount)/float64(length))

	row = db.QueryRow("SELECT COUNT(*) FROM response_times WHERE response_time <= 0.5")
	row.Scan(&responseCount)
	fmt.Printf("500ms and under: %.2f%%\n", float64(responseCount)/float64(length))

	row = db.QueryRow("SELECT COUNT(*) FROM response_times WHERE response_time > 0.5")
	row.Scan(&responseCount)
	fmt.Printf("Above 500ms: %.2f%%\n", float64(responseCount)/float64(length))
	wg.Wait()
}

// gets environment variable DURATION in minutes
func getDuration() float64 {
	duration, err := strconv.Atoi(os.Getenv("DURATION"))
	if err != nil {
		log.Fatal("GD Atoi error: ", err)
	}
	return float64(duration)
}

func Worker(identifier string, revision Revision, count *Count, wg *sync.WaitGroup,
	slp int, db *sql.DB) {
	defer wg.Done()
	if slp != 0 {
		time.Sleep(time.Duration(slp) * time.Second)
	}

	startTime := time.Now()

	err := getRiskScore(identifier, revision)
	if err != nil {
		log.Print(err)
		return
	}

	responseTime := time.Since(startTime).Seconds()
	count.Increment()

	statement, _ := db.Prepare("INSERT INTO response_times (revision_id, project_id, response_time) VALUES (?, ?, ?)")
	statement.Exec(revision.ID, identifier, responseTime)
}

func getRiskScore(identifier string, revision Revision) error {
	bdy := map[string]interface{}{
		"rev_id": revision.ID,
		"lang":   identifier,
	}
	jsonData, err := json.Marshal(bdy)
	if err != nil {
		log.Print("json marshall error: ", err)
	}
	req, _ := http.NewRequest("POST",
		"https://api.wikimedia.org/service/lw/inference/v1/models/revertrisk-language-agnostic:predict",
		bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("ACCESS_TOKEN"))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Risk Error: %s", err)
		return err
	}

	defer resp.Body.Close()

	return nil
}

func calculateSleep(count float64) int {
	cMax, err := strconv.Atoi(os.Getenv("LIMIT"))
	cFloat := float64(cMax)
	if err != nil {
		log.Fatal("CS Atoi error: ", err)
	}
	if (count > 0.07*cFloat && count < 0.10*cFloat) ||
		(count > 0.17*cFloat && count < 0.20*cFloat) ||
		(count > 0.27*cFloat && count < 0.30*cFloat) ||
		(count > 0.37*cFloat && count < 0.40*cFloat) ||
		(count > 0.47*cFloat && count < 0.50*cFloat) ||
		(count > 0.57*cFloat && count < 0.60*cFloat) ||
		(count > 0.67*cFloat && count < 0.70*cFloat) ||
		(count > 0.77*cFloat && count < 0.80*cFloat) ||
		(count > 0.87*cFloat && count < 0.90*cFloat) {
		return linearModel(count)
	}

	return 0
}

func getLimit() float64 {
	cMax, err := strconv.Atoi(os.Getenv("LIMIT"))
	if err != nil {
		log.Fatal("GL Atoi error: ", err)
	}
	cFloat := float64(cMax)
	return cFloat
}

func linearModel(count float64) int {
	duration := 5 * 60.0 // 5 minute sleep delay is the max
	limit := getLimit()

	spikeFraction := 0.03

	gradient := math.Floor((0.0-duration)/(limit*spikeFraction) - 0.0)
	yIntercept := duration
	return int(gradient*count + yIntercept)
}
