package main

import (
	"errors"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const TimeColumnName = "time"

var ErrInvalidTimeColumn = errors.New("key can't be time")

type Column struct {
	schema record.Field
	cv     record.ColVal
}

type Transform struct {
	Database        string
	RetentionPolicy string
	Measurement     string
	RowCount        int64
	MinTime         int64
	MaxTime         int64
	mux             sync.RWMutex
	Columns         map[string]*Column
}

func NewTransform(database, rp, mst string) *Transform {
	return &Transform{Database: database, RetentionPolicy: rp, Measurement: mst, Columns: make(map[string]*Column)}
}

// AppendLine write by row, traverse all fields and append their values to Columns
func (r *Transform) AppendLine(tags map[string]string, fields map[string]interface{}, timestamp int64) {
	r.mux.Lock()
	defer r.mux.Unlock()

	// processing tag columns
	r.getTagColumns(tags)

	// processing field columns
	r.getFieldColumns(fields)

	// processing time column
	if timestamp == 0 {
		timestamp = int64(time.Now().Nanosecond())
	}
	timeCol, ok := r.Columns[TimeColumnName]
	if !ok {
		timeCol = &Column{
			schema: record.Field{
				Name: TimeColumnName,
				Type: influx.Field_Type_Int,
			},
			cv: record.ColVal{},
		}
		timeCol.cv.Init()
		timeCol.cv.AppendIntegerNulls(int(r.RowCount))
	}
	r.Columns[TimeColumnName].cv.AppendInteger(timestamp)
	if timestamp > r.MaxTime {
		r.MaxTime = timestamp
	}
	if timestamp < r.MinTime {
		r.MinTime = timestamp
	}

	// after successful processing, the number of record rows +1
	r.RowCount++
}

func (r *Transform) getTagColumns(tags map[string]string) {
	for tagName, tagValue := range tags {
		if tagName == TimeColumnName {
			panic(ErrInvalidTimeColumn)
		}
		tagColumn, ok := r.Columns[tagName]
		// create Column because it does not exist
		if !ok {
			tagColumn = &Column{
				schema: record.Field{
					Name: tagName,
					Type: influx.Field_Type_Tag,
				},
				cv: record.ColVal{},
			}
			tagColumn.cv.Init()
			// for new tags, missing nil fields need to be filled
			tagColumn.cv.AppendStringNulls(int(r.RowCount))
		}
		// write the tag value to column
		tagColumn.cv.AppendString(tagValue)
		r.Columns[tagName] = tagColumn
	}
}

func (r *Transform) getFieldColumns(fields map[string]interface{}) {
	for fieldName, fieldValue := range fields {
		if fieldName == TimeColumnName {
			panic(ErrInvalidTimeColumn)
		}
		fieldColumn, ok := r.Columns[fieldName]
		// create Column because it does not exist
		if !ok {
			switch fieldValue.(type) {
			case string:
				fieldColumn = &Column{
					schema: record.Field{
						Name: fieldName,
						Type: influx.Field_Type_String,
					},
					cv: record.ColVal{},
				}
				fieldColumn.cv.Init()
				fieldColumn.cv.AppendStringNulls(int(r.RowCount))
			case bool:
				fieldColumn = &Column{
					schema: record.Field{
						Name: fieldName,
						Type: influx.Field_Type_Boolean,
					},
					cv: record.ColVal{},
				}
				fieldColumn.cv.Init()
				fieldColumn.cv.AppendBooleanNulls(int(r.RowCount))
			case float64, float32:
				fieldColumn = &Column{
					schema: record.Field{
						Name: fieldName,
						Type: influx.Field_Type_Float,
					},
					cv: record.ColVal{},
				}
				fieldColumn.cv.Init()
				fieldColumn.cv.AppendFloatNulls(int(r.RowCount))
			case int, int64, int32, uint, uint32, uint64:
				fieldColumn = &Column{
					schema: record.Field{
						Name: fieldName,
						Type: influx.Field_Type_Int,
					},
				}
				fieldColumn.cv.Init()
				fieldColumn.cv.AppendIntegerNulls(int(r.RowCount))
			}
		}
		// TODO extract this logic into a method
		switch fieldValue.(type) {
		case string:
			fieldColumn.cv.AppendString(fieldValue.(string))
		case bool:
			fieldColumn.cv.AppendBoolean(fieldValue.(bool))
		case float64, float32:
			fieldColumn.cv.AppendFloat(fieldValue.(float64))
		case int, int64, int32, uint, uint32, uint64:
			// TODO Later, the interface is converted to int64
			fieldColumn.cv.AppendInteger(fieldValue.(int64))
		}
		// write the record to column
		r.Columns[fieldName] = fieldColumn
	}
}

// ToSrvRecords convert to the structure record.Record required by the server
func (r *Transform) ToSrvRecords() *record.Record {
	var rec = &record.Record{}
	for _, column := range r.Columns {
		rec.Schema = append(rec.Schema, column.schema)
		rec.ColVals = append(rec.ColVals, column.cv)
	}
	// data sorting
	rec = record.NewColumnSortHelper().Sort(rec)
	return rec
}
