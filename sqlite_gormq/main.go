package main

import (
	"fmt"
	"log"
	"os"
	"time"

	svsignaldb "github.com/lasaleks/selDataBase/svsignal_db"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func FillSignals(db *gorm.DB) {

	gr := &svsignaldb.Group{Model: gorm.Model{ID: 1}, Name: "Группа"}
	db.Create(&gr)
	for i := 0; i < SIGNALS_NOF; i++ {
		db.Create(&svsignaldb.Signal{Model: gorm.Model{ID: 1}, GroupID: gr.ID, Name: fmt.Sprintf("Signal%d", i), Key: fmt.Sprintf("Signal.%d", i), Period: 60})
	}
}

var value_id = 0

func timer(name string) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)
		seconds := int64(duration.Milliseconds())
		var rows_per_sec int64
		if seconds > 0 {
			rows_per_sec = int64(value_id) * 1000 / seconds
		} else {
			rows_per_sec = 0
		}
		fmt.Printf("%s took %v\nrows = %d\nrows/sec = %d\n", name, time.Since(start), value_id, rows_per_sec)
	}
}

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

func test1() {
	value_id = 0
	os.Remove("test1.db")
	db, err := gorm.Open(sqlite.Open("test1.db"), &config)
	if err != nil {
		panic("failed to connect database")
	}
	for _, exec := range EXECS {
		db.Exec(exec)
	}

	// Migrate the schema
	svsignaldb.Migrate(db)
	FillSignals(db)
	fmt.Println("------test1---- bulk insert")
	defer timer("test1")()
	//value_id := 0
	pack_size := BULK_INSERT_SIZE
	values := make([]svsignaldb.FValue, pack_size)
	for j := 0; j < CYCLE; j++ {
		for signal_id := 1; signal_id <= SIGNALS_NOF; signal_id++ {
			//tx := db.Begin()
			for i := 0; i < pack_size; i++ {
				value_id++
				values[i] = svsignaldb.FValue{ID: value_id, SignalID: uint(signal_id), UTime: int64(signal_id), Value: float64(signal_id)}
			}
			db.Create(values)
			//tx.Create(values)
			//tx.Commit()
		}
	}
}

func test2() {
	value_id = 0
	os.Remove("test2.db")
	db, err := gorm.Open(sqlite.Open("test2.db"), &config)
	if err != nil {
		panic("failed to connect database")
	}
	for _, exec := range EXECS {
		db.Exec(exec)
	}

	// Migrate the schema
	svsignaldb.Migrate(db)
	FillSignals(db)
	fmt.Println("------test2---- bulk insert + transaction")
	defer timer("test2")()
	//value_id := 0
	pack_size := BULK_INSERT_SIZE
	values := make([]svsignaldb.FValue, pack_size)
	for j := 0; j < CYCLE; j++ {
		for signal_id := 1; signal_id <= SIGNALS_NOF; signal_id++ {
			tx := db.Begin()
			for i := 0; i < pack_size; i++ {
				value_id++
				values[i] = svsignaldb.FValue{ID: value_id, SignalID: uint(signal_id), UTime: int64(signal_id), Value: float64(signal_id)}
			}
			tx.Create(values)
			tx.Commit()
		}
	}
}

func test3() {
	name_db := "test3.db"
	value_id = 0
	os.Remove(name_db)
	db, err := gorm.Open(sqlite.Open(name_db), &config)
	if err != nil {
		panic("failed to connect database")
	}
	for _, exec := range EXECS {
		db.Exec(exec)
	}

	// Migrate the schema
	svsignaldb.Migrate(db)
	FillSignals(db)
	fmt.Println("------" + name_db + "---- transaction")
	defer timer(name_db)()
	//value_id := 0
	pack_size := BULK_INSERT_SIZE
	for j := 0; j < CYCLE; j++ {
		for signal_id := 1; signal_id <= SIGNALS_NOF; signal_id++ {
			tx := db.Begin()
			for i := 0; i < pack_size; i++ {
				value_id++
				tx.Create(&svsignaldb.FValue{ID: value_id, SignalID: uint(signal_id), UTime: int64(signal_id), Value: float64(signal_id)})
			}

			tx.Commit()
		}
	}
}

func open_db(name_db string) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(name_db), &config)
	if err != nil {
		panic("failed to connect database")
	}
	for _, exec := range EXECS {
		db.Exec(exec)
	}
	return db
}

func test4(db *gorm.DB, name_db string) {
	value_id = 0
	db.Last(&svsignaldb.FValue{}, &value_id)
	// Migrate the schema
	svsignaldb.Migrate(db)
	FillSignals(db)
	fmt.Println("------" + name_db + "----")
	defer timer(name_db)()
	//value_id := 0
	pack_size := BULK_INSERT_SIZE
	for j := 0; j < CYCLE; j++ {
		for signal_id := 1; signal_id <= SIGNALS_NOF; signal_id++ {
			for i := 0; i < pack_size; i++ {
				value_id++
				db.Create(svsignaldb.FValue{ID: value_id, SignalID: uint(signal_id), UTime: int64(signal_id), Value: float64(signal_id)})
			}
		}
	}
}

const BULK_INSERT_SIZE = 1000
const CYCLE = 50
const SIGNALS_NOF = 100

var config = gorm.Config{
	//PrepareStmt: true,
	//SkipDefaultTransaction: true,
	Logger: new_logger,
}

var EXECS = []string{
	"PRAGMA journal_mode = WAL",
	"PRAGMA synchronous = NORMAL",
}

func main() {
	fmt.Printf("BULK_INSERT_SIZE = %d\nCYCLE = %d\nSIGNALS_NOF = %d\n", BULK_INSERT_SIZE, CYCLE, SIGNALS_NOF)
	fmt.Printf("%#v\n", EXECS)
	fmt.Printf("%+v\n", config)
	//test1()
	//test2()
	//test3()
	test4(open_db("test4.db"), "test4.db")
}
