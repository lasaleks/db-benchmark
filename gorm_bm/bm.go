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

func CreateSignal(db *gorm.DB, nof int) {
	gr := &svsignaldb.Group{Model: gorm.Model{ID: 1}, Name: "Группа"}
	db.Create(&gr)
	for i := 0; i < nof; i++ {
		db.Create(&svsignaldb.Signal{Model: gorm.Model{ID: uint(i)}, GroupID: gr.ID, Name: fmt.Sprintf("Signal%d", i), Key: fmt.Sprintf("Signal.%d", i), Period: 60})
	}
}

func InitBenchMark(db *gorm.DB, nofsignals int) {
	gr := &svsignaldb.Group{Model: gorm.Model{ID: 1}, Name: "Группа"}
	db.Create(&gr)
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
	for atomic.LoadInt32(&stop_bm) == 0 {
		for _, signal := range option.ListSignal {
			if atomic.LoadInt32(&stop_bm) != 0 {
				break
			}
			for i := 0; i < option.SignalWriteNOfRows; i++ {
				value_id++
				insert_rows++
				values[idx_value] = svsignaldb.FValue{ID: value_id, SignalID: signal.ID, UTime: utime_value, Value: 0}
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
						if insert_rows%(option.BulkSize*100) == 0 {
							fmt.Println("write", insert_rows)
						}
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
	Begin        int64
	End          int64
	Limit        int
	ListSignal   []*svsignaldb.Signal
	PrintProcess bool
}

func BenchmarkRead(wg *sync.WaitGroup, db *gorm.DB, option OptionBMRead) {
	defer wg.Done()
	start := time.Now()
	read_values := 0
	values := []svsignaldb.FValue{}
	begin_period := option.Begin
	end_period := begin_period + int64(option.Limit)
	for atomic.LoadInt32(&stop_bm) == 0 {
		for _, signal := range option.ListSignal {
			if atomic.LoadInt32(&stop_bm) != 0 {
				break
			}
			res := db.Where("signal_id = ? and utime>=? and utime<=?", signal.ID, begin_period, end_period).Limit(option.Limit - 1).Find(&values)
			if res.Error != nil {
				log.Println(res.Error)
			}
			//fmt.Println(signal.ID, begin_period, end_period, len(values))
			//db.Where("utime>=? and utime<=?", begin_utime, begin_utime+10000).Find(&values)
			read_values += len(values)
		}
		if begin_period+int64(option.Limit) < option.End {
			begin_period += int64(option.Limit)
		} else {
			begin_period = int64(option.Limit)
		}
		end_period = begin_period + int64(option.Limit)
		if option.PrintProcess {
			fmt.Printf("begin:%d end:%d read %d\n", begin_period, end_period, read_values)
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
