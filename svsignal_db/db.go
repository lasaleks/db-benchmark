package svsignaldb

import (
	"log"

	"gorm.io/gorm"
)

type Group struct {
	gorm.Model
	Key     string
	Name    string
	Signals []Signal
}

type Signal struct {
	gorm.Model
	GroupID  uint
	Key      string
	Name     string
	TypeSave int8
	Period   int
	Delta    float32
	Tags     []Tag
	//MValues  []MValue
}

type MValue struct {
	ID       int
	SignalID uint  `gorm:"index"`
	UTime    int64 `gorm:"index;column:utime;"`
	Max      float32
	Min      float32
	Mean     float32
	Median   float32
	OffLine  bool
}

type IValue struct {
	ID       int
	SignalID uint  `gorm:"index"`
	UTime    int64 `gorm:"index;column:utime;"`
	Value    int32
	OffLine  bool
}

type FValue struct {
	ID       int
	SignalID uint  `gorm:"index"`
	UTime    int64 `gorm:"index;column:utime;"`
	Value    float64
	OffLine  bool
}

/*type FValue struct {
	ID       int
	SignalID uint  `gorm:"index:idx_sig_utime,priority:2;"`
	UTime    int64 `gorm:"index:idx_sig_utime,priority:1;column:utime;"`
	Value    float64
	OffLine  bool
}*/

func (FValue) TableName() string {
	return "f_values"
}

type Tag struct {
	gorm.Model
	SignalID uint
	Tag      string
	Value    string
}

func Migrate(db *gorm.DB) {
	if err := db.AutoMigrate(&Group{}, &Signal{}, &MValue{}, &IValue{}, &FValue{}, &Tag{}); err != nil {
		log.Panicln(err)
	}
}
