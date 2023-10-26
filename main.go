package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	gormbm "github.com/lasaleks/db-benchmark/gorm_bm"
	svsignaldb "github.com/lasaleks/db-benchmark/svsignal_db"
	goutils "github.com/lasaleks/go-utils"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var new_logger = logger.New(
	log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
	logger.Config{
		SlowThreshold:             time.Second,   // Slow SQL threshold
		LogLevel:                  logger.Silent, // Log level
		IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
		ParameterizedQueries:      true,          // Don't include params in the SQL log
		Colorful:                  false,         // Disable color
	},
)

var config = gorm.Config{
	//PrepareStmt:            true,
	//SkipDefaultTransaction: true,
	Logger: new_logger,
}

var EXECS = []string{
	"PRAGMA journal_mode = WAL",
	"PRAGMA synchronous = OFF",
}

var migrate = flag.Bool("migrate", false, "")
var signal_nof = flag.Int("signal-nof", 1000, "")
var signal_write_nof_rows = flag.Int("signal-write-nof-rows", 10, "")
var bulkSize = flag.Int("bulk-size", 1000, "")
var read_limit = flag.Int("limit", 1000, "")
var read = flag.Bool("read", false, "")
var write = flag.Bool("write", false, "")
var printProcess = flag.Bool("print-process", false, "")
var typeDB = flag.String("type-db", "sqlite", "sqlite/mysql")
var pathDB = flag.String("url", "svsignal_bm.db", "")
var hostDB = flag.String("host", "localhost", "")

func main() {
	var wg sync.WaitGroup
	ctx := context.Background()
	flag.Parse()
	var db *gorm.DB
	var err error
	fmt.Printf("BULK_INSERT_SIZE = %d\nSIGNALS_NOF = %d\n", *bulkSize, *signal_nof)
	switch *typeDB {
	case "sqlite":
		db, err = gorm.Open(sqlite.Open(*pathDB), &config)
		if err != nil {
			panic("failed to connect database")
		}
		for _, exec := range EXECS {
			fmt.Println(exec)
			db.Exec(exec)
		}
	case "mysql":
		url := fmt.Sprintf("bm:bm@tcp(%s:3306)/benchmark?charset=utf8&parseTime=True&loc=Local", *hostDB)
		db, err = gorm.Open(mysql.Open(url), &config)
		if err != nil {
			log.Panicln("failed to connect database", url)
		}
	default:
		log.Panicln("typeDB error value:", *typeDB)
	}

	if *migrate {
		fmt.Println("Migrate data base")
		svsignaldb.Migrate(db)
	}
	gormbm.InitBenchMark(db, *signal_nof)
	signals := gormbm.GetListSignal(db)
	signals = signals[0:*signal_nof]
	if *write {
		wg.Add(1)
		go gormbm.BenchmarkWrite(&wg, db,
			gormbm.OptionBMWrite{
				BulkSize:           *bulkSize,
				ListSignal:         signals,
				PrintProcess:       *printProcess,
				SignalWriteNOfRows: *signal_write_nof_rows,
			})
	}
	if *read {
		wg.Add(1)
		go gormbm.BenchmarkRead(&wg, db, gormbm.OptionBMRead{
			Limit:        *read_limit,
			ListSignal:   signals,
			PrintProcess: *printProcess,
		})
		//*read_begin, *read_end, *read_limit, signals)
	}

	f_shutdown := func(ctx context.Context) {
		fmt.Println("ShutDown")
		gormbm.Stop()

	}
	wg.Add(1)
	go goutils.WaitSignalExit(&wg, ctx, f_shutdown)

	wg.Wait()
	//test3(open_db("test_transaction.db"), "test_transaction.db")
}
