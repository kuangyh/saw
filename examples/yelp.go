package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/kuangyh/saw"
	"github.com/kuangyh/saw/table"
	"golang.org/x/net/context"
	"io"
	"log"
	"net/http"
	"time"

	_ "net/http/pprof"
)

type YelpReview struct {
	Stars  float64 `json:"stars"`
	BizId  string  `json:"business_id"`
	UserId string  `json:"user_id"`
	Text   string  `json:"text"`
}

type Sum struct {
	curr float64
}

func (sum *Sum) Emit(datum saw.Datum) error {
	sum.curr += datum.Value.(float64)
	return nil
}

func (sum *Sum) Result(ctx context.Context) (interface{}, error) {
	return sum.curr, nil
}

func SumItemFactory(name string, key saw.DatumKey) (saw.Saw, error) {
	return &Sum{}, nil
}

type YelpHandler struct{}

func (yh *YelpHandler) parseReview(datum saw.Datum) YelpReview {
	line := datum.Value.([]byte)
	review := YelpReview{}
	if err := json.Unmarshal(line, &review); err != nil {
		log.Panic(err)
	}
	return review
}

func (yh *YelpHandler) Emit(datum saw.Datum) error {
	review := yh.parseReview(datum)
	bizSumTable.Emit(saw.Datum{
		Key:   saw.DatumKey(review.BizId),
		Value: float64(1),
	})
	reviewByUserTable.Emit(saw.Datum{
		Key:   saw.DatumKey(review.UserId),
		Value: review.Text,
	})
	return nil
}

func (yh *YelpHandler) Result(ctx context.Context) (interface{}, error) {
	return nil, nil
}

var (
	bizSumTable = table.NewMemTable(table.TableSpec{
		Name:           "bizSumTable",
		ItemFactory:    SumItemFactory,
		PersistentPath: "bizSumTable",
		ValueEncoder:   saw.JSONEncoder,
	})
	reviewByUserTable saw.Saw

	yelpHandler = &YelpHandler{}
)

type stringEncoder struct{}

func (se stringEncoder) EncodeValue(value interface{}, w io.Writer) (err error) {
	_, err = w.Write([]byte(value.(string)))
	return
}

func init() {
	var err error
	if reviewByUserTable, err = table.NewCollectionTable(table.TableSpec{
		Name:           "reviewByUserTable",
		PersistentPath: "reviewByUserTable",
		ValueEncoder:   stringEncoder{},
	}); err != nil {
		log.Panic(err)
	}
}

func loadFile(filename string, shards int) error {
	file, err := saw.OpenGCSFile(context.Background(), filename)
	if err != nil {
		return err
	}
	defer file.Close()

	qg := saw.QueueGroup{}
	par := qg.NewPar(shards, 10000)
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		par.Sched(func() {
			yelpHandler.Emit(saw.Datum{Value: line})
		})
	}
	qg.Join()
	return nil
}

func main() {
	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	startTime := time.Now()
	loadFile("/xv-dev/yelp-data/review.log", 256)
	fmt.Println("Done", time.Since(startTime))
	result, err := bizSumTable.Result(context.Background())
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(len(result.(table.TableResultMap)))
}
