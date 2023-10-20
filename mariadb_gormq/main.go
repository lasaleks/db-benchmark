package main

import (
	"fmt"
	"log"
	"os"
	"time"

	svsignaldb "github.com/lasaleks/db-benchmark/svsignal_db"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func FillSignals(db *gorm.DB) {
	gr := &svsignaldb.Group{Model: gorm.Model{ID: 1}, Name: "Группа"}
	db.Create(&gr)
	for i := 1; i <= SIGNALS_NOF; i++ {
		db.Create(&svsignaldb.Signal{Model: gorm.Model{ID: uint(i)}, GroupID: gr.ID, Name: fmt.Sprintf("Signal%d", i), Key: fmt.Sprintf("Signal.%d", i), Period: 60})
	}
}

var rows_record = 0

func timer(name string) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)
		seconds := int64(duration.Milliseconds())
		var rows_per_sec int64
		if seconds > 0 {
			rows_per_sec = int64(rows_record) * 1000 / seconds
		} else {
			rows_per_sec = 0
		}
		fmt.Printf("%s took %v\nrows = %d\nrows/sec = %d\n", name, time.Since(start), rows_record, rows_per_sec)
	}
}

var new_logger = logger.New(
	log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
	logger.Config{
		SlowThreshold:             time.Second * 10, // Slow SQL threshold
		LogLevel:                  logger.Silent,    // Log level
		IgnoreRecordNotFoundError: true,             // Ignore ErrRecordNotFound error for logger
		ParameterizedQueries:      true,             // Don't include params in the SQL log
		Colorful:                  false,            // Disable color
	},
)

func test1(db *gorm.DB, name_db string) {
	rows_record = 0
	// Migrate the schema
	svsignaldb.Migrate(db)
	FillSignals(db)
	value_id := 0
	fval := svsignaldb.FValue{}
	res := db.Last(&fval)
	///res := db.Last(&svsignaldb.FValue{}, &value_id)
	if res.Error != nil {
		log.Printf("GetLastId", res.Error.Error())
	} else {
		value_id = fval.ID
	}
	fmt.Println("------test2---- bulk insert + transaction")
	defer timer(name_db)()
	pack_size := BULK_INSERT_SIZE
	values := make([]svsignaldb.FValue, pack_size)
	for j := 0; j < CYCLE; j++ {
		for signal_id := 1; signal_id <= SIGNALS_NOF; signal_id++ {
			for i := 0; i < pack_size; i++ {
				value_id++
				rows_record++
				values[i] = svsignaldb.FValue{ID: value_id, SignalID: uint(signal_id), UTime: int64(signal_id), Value: float64(signal_id)}
			}
			db.Create(values)
		}
	}
}

func test2(db *gorm.DB) {
	rows_record = 0
	// Migrate the schema
	svsignaldb.Migrate(db)
	FillSignals(db)
	value_id := 0
	fval := svsignaldb.FValue{}
	res := db.Last(&fval)
	///res := db.Last(&svsignaldb.FValue{}, &value_id)
	if res.Error != nil {
		log.Printf("GetLastId", res.Error.Error())
	} else {
		value_id = fval.ID
	}
	fmt.Println("------test2---- bulk insert + transaction")
	defer timer("")()
	pack_size := BULK_INSERT_SIZE
	values := make([]svsignaldb.FValue, pack_size)
	for j := 0; j < CYCLE; j++ {
		for signal_id := 1; signal_id <= SIGNALS_NOF; signal_id++ {
			tx := db.Begin()
			for i := 0; i < pack_size; i++ {
				value_id++
				rows_record++
				values[i] = svsignaldb.FValue{ID: value_id, SignalID: uint(signal_id), UTime: int64(signal_id), Value: float64(signal_id)}
			}
			tx.Create(values)
			tx.Commit()
		}
	}
}

func open_db() *gorm.DB {
	dsn := "apache2:apache2data@tcp(localhost:3306)/benchmark?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &config)

	//db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	/*
		for _, exec := range EXECS {
			db.Exec(exec)
		}*/
	return db
}

const BULK_INSERT_SIZE = 1000
const CYCLE = 10
const SIGNALS_NOF = 1000

var config = gorm.Config{
	//PrepareStmt: true,
	//SkipDefaultTransaction: true,
	Logger: new_logger,
}

var EXECS = []string{
	"PRAGMA journal_mode = WAL",
	"PRAGMA synchronous = OFF",
}

func main() {
	fmt.Printf("BULK_INSERT_SIZE = %d\nCYCLE = %d\nSIGNALS_NOF = %d\n", BULK_INSERT_SIZE, CYCLE, SIGNALS_NOF)
	fmt.Printf("%#v\n", EXECS)
	fmt.Printf("%+v\n", config)
	//test1(open_db("test_bulk.db"), "test_bulk.db")
	test2(open_db())
	//test3(open_db("test_transaction.db"), "test_transaction.db")
}
