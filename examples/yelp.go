package main

import (
	"fmt"
	"github.com/kuangyh/saw"
	"github.com/kuangyh/saw/aggregator"
	"github.com/kuangyh/saw/runner"
	"github.com/kuangyh/saw/storage"
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

type YelpHandler struct{}

func (yh *YelpHandler) Emit(datum saw.Datum) error {
	review := datum.Value.(*YelpReview)
	bizSumTable.Emit(saw.Datum{
		Key:   saw.DatumKey(review.BizId),
		Value: aggregator.Metric(1),
	})
	// for i := 0; i < 5; i++ {
	reviewByUserTable.Emit(saw.Datum{
		Key:   saw.DatumKey(review.UserId),
		Value: []byte(review.Text),
	})
	// }
	return nil
}

func (yh *YelpHandler) Result(ctx context.Context) (interface{}, error) {
	return nil, nil
}

var (
	inputTopic        = saw.TopicID("input")
	yelpHandler       YelpHandler
	bizSumTable       saw.Saw
	reviewByUserTable saw.Saw
)

type stringEncoder struct{}

func (se stringEncoder) EncodeValue(value interface{}, w io.Writer) (err error) {
	_, err = w.Write([]byte(value.(string)))
	return
}

func init() {
	saw.GlobalHub.Register(&yelpHandler, inputTopic)

	bizSumTableOutput := storage.MustParseResourcePath(
		"recordkv:/gs/xv-dev/output/bizSumTable.recordio")
	bizSumTable = table.NewMemTable(table.TableSpec{
		Name:               "bizSumTable",
		PersistentResource: bizSumTableOutput,
		ItemFactory:        table.ItemFactoryOf(&aggregator.Sum{}),
		ValueEncoder:       saw.JSONEncoder{},
	})

	reviewByUserTableOutput := storage.MustParseResourcePath(
		"recordkv:/gs/xv-dev/output/reviewByUserTable.recordio@64")
	var err error
	if reviewByUserTable, err = table.NewCollectTable(context.Background(), table.TableSpec{
		Name:               "reviewByUserTable",
		PersistentResource: reviewByUserTableOutput,
	}); err != nil {
		log.Panic(err)
	}
}

func main() {
	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	batch := runner.BatchSpec{
		Input:             storage.MustParseResourcePath("textio:/gs/xv-dev/yelp-data/review.log"),
		InputValueDecoder: saw.NewJSONDecoder(&YelpReview{}),
		Topic:             inputTopic,
		NumShards:         64,
		QueueBufferSize:   1000,
	}

	startTime := time.Now()
	runner.RunBatch(batch)
	fmt.Println("Done", time.Since(startTime))

	result, err := bizSumTable.Result(context.Background())
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(len(result.(table.TableResultMap)))

	rc, _ := reviewByUserTable.Result(context.Background())
	fmt.Println(rc)
}
