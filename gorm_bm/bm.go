package gormbm

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	svsignaldb "github.com/lasaleks/db-benchmark/svsignal_db"
	"gorm.io/gorm"
)

func InitBenchMark(db *gorm.DB, nofsignals int) {
	gr := &svsignaldb.Group{Model: gorm.Model{ID: 1}, Name: "Группа"}
	res := db.Create(&gr)
	if res.Error != nil {
		log.Println(res.Error)
	}
	for i := 1; i <= nofsignals; i++ {
		db.Create(&svsignaldb.Signal{Model: gorm.Model{ID: uint(i)}, GroupID: gr.ID, Name: fmt.Sprintf("Signal%d", i), Key: fmt.Sprintf("Signal.%d", i), Period: 60})
	}
}

func GetListSignal(db *gorm.DB) []*svsignaldb.Signal {
	var signals []*svsignaldb.Signal
	if res := db.Find(&signals); res.Error != nil {
		log.Println("GetAllSignal err:", res.Error)
	}
	return signals
}

type OptionBMWrite struct {
	BulkSize           int
	ListSignal         []*svsignaldb.Signal
	PrintProcess       bool
	SignalWriteNOfRows int
}

func BenchmarkWrite(wg *sync.WaitGroup, db *gorm.DB, option OptionBMWrite) {
	defer wg.Done()
	// Migrate the schema
	fval := svsignaldb.FValue{}
	res := db.Last(&fval)
	value_id := 0
	if res.Error != nil {
		log.Println("get last value_id err:", res.Error.Error())
	} else {
		value_id = fval.ID
	}

	row := db.Table(svsignaldb.FValue{}.TableName()).Select("max(utime)").Row()
	var max_utime int64
	row.Scan(&max_utime)

	insert_rows := 0
	values := make([]svsignaldb.FValue, option.BulkSize)
	start := time.Now()
	var utime_value int64 = max_utime + 1
	idx_value := 0
	tx := db.Begin()
	start_cmt := start
	for atomic.LoadInt32(&stop_bm) == 0 {
		for _, signal := range option.ListSignal {
			if atomic.LoadInt32(&stop_bm) != 0 {
				break
			}
			for i := 0; i < option.SignalWriteNOfRows; i++ {
				value_id++
				insert_rows++
				values[idx_value] = svsignaldb.FValue{ID: value_id, SignalID: signal.ID, UTime: utime_value + int64(i), Value: 0}
				idx_value++
				if idx_value >= option.BulkSize {
					idx_value = 0
					res := tx.Create(values)
					if res.Error != nil {
						log.Println("insert error:", res.Error)
					}
					res = tx.Commit()
					if res.Error != nil {
						log.Println("commit error:", res.Error)
					}
					tx = db.Begin()
					if option.PrintProcess {
						fmt.Printf("write rows:%d took:%v\n", insert_rows, time.Since(start_cmt))
						start_cmt = time.Now()
					}
				}
			}
		}
		utime_value += int64(option.SignalWriteNOfRows)
	}
	if idx_value != 0 {
		tx.Create(values[:idx_value])
		tx.Commit()
		tx.Begin()
	}
	duration := time.Since(start)
	msec := int64(duration.Milliseconds())
	fmt.Println("write rows:", insert_rows)
	fmt.Printf("write took %v\n", duration)
	var rows_per_sec int64
	if msec > 0 {
		rows_per_sec = int64(insert_rows) * 1000 / msec
	} else {
		rows_per_sec = 0
	}
	fmt.Printf("write rows/sec:%v\n", rows_per_sec)
	fmt.Println("last utime:", max_utime)
}

type OptionBMRead struct {
	Limit        int
	ListSignal   []*svsignaldb.Signal
	PrintProcess bool
}

type QuerySignal struct {
	prevId int
	begin  int64
	end    int64
}

func BenchmarkRead(wg *sync.WaitGroup, db *gorm.DB, option OptionBMRead) {
	defer wg.Done()

	var count int64
	db.Table(svsignaldb.FValue{}.TableName()).Count(&count)
	fmt.Println("count values:", count)

	query_signals := map[uint]*QuerySignal{}

	start := time.Now()
	read_values := 0
	values := []svsignaldb.FValue{}
	start_cmt := start
	for atomic.LoadInt32(&stop_bm) == 0 {
		for _, signal := range option.ListSignal {
			if atomic.LoadInt32(&stop_bm) != 0 {
				break
			}
			query, ok := query_signals[signal.ID]
			if !ok {
				query = &QuerySignal{
					begin: 0,
					end:   int64(option.Limit),
				}
				query_signals[signal.ID] = query
			}
			if option.PrintProcess {
				fmt.Printf("signal_id:%d begin:%d end:%d", signal.ID, query.begin, query.end)
			}
			res := db.Where("signal_id = ? and utime>=? and utime<?", signal.ID, query.begin, query.end).Limit(option.Limit).Find(&values)
			if res.Error != nil {
				log.Println(res.Error)
			}

			if len(values) > 0 {
				val := &values[len(values)-1]
				if val.ID == query.prevId {
					query.begin = val.UTime + 1
				} else {
					query.begin = val.UTime
				}
				query.end = val.UTime + int64(option.Limit)
				query.prevId = val.ID
			} else {
				query.begin = 0
				query.end = int64(option.Limit)
			}
			read_values += len(values)
			if option.PrintProcess {
				fmt.Printf(" +(%d) read %d took:%v\n", len(values), read_values, time.Since(start_cmt))
				start_cmt = time.Now()
			}
		}
	}
	duration := time.Since(start)
	msec := int64(duration.Milliseconds())
	fmt.Println("read rows:", read_values)
	fmt.Printf("read took %v\n", duration)
	var rows_per_sec int64
	if msec > 0 {
		rows_per_sec = int64(read_values) * 1000 / msec
	} else {
		rows_per_sec = 0
	}
	fmt.Printf("read rows/sec:%v\n", rows_per_sec)

}

// range specification, note that min <= max
type IntRange struct {
	min, max int
}

// get next random value within the interval including min and max
func (ir *IntRange) NextRandom(r *rand.Rand) int {
	return r.Intn(ir.max-ir.min+1) + ir.min
}

var randinit = rand.New(rand.NewSource(55))

func randIntRange(b int, e int) int {
	ir := IntRange{b, e}
	return ir.NextRandom(randinit)
}

var stop_bm int32

func Stop() {
	atomic.AddInt32(&stop_bm, 1)
}
