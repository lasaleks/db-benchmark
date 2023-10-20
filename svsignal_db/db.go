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
	UTime    int64 `gorm:"index"`
	Max      float32
	Min      float32
	Mean     float32
	Median   float32
	OffLine  bool
}

type IValue struct {
	ID       int
	SignalID uint
	UTime    int64 `gorm:"index"`
	Value    int32 `gorm:"index"`
	OffLine  bool
}

type FValue struct {
	ID       int
	SignalID uint  `gorm:"index"`
	UTime    int64 `gorm:"index"`
	Value    float64
	OffLine  bool
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
