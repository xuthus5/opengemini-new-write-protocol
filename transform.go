package main

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	TimeColumnName = "time"
)

var (
	ErrInvalidTimeColumn = errors.New("key can't be time")
	ErrEmptyName         = errors.New("empty name not allowed")
	ErrInvalidFieldType  = errors.New("invalid field type")
	ErrUnknownFieldType  = errors.New("unknown field type")
)

type Column struct {
	schema record.Field
	cv     record.ColVal
}

type Transform struct {
	Database        string
	RetentionPolicy string
	Measurement     string
	RowCount        int
	MinTime         int64
	MaxTime         int64
	mux             sync.RWMutex
	Columns         map[string]*Column
	fillChecker     map[string]bool
}

// NewTransform creates a new Transform instance with configuration
func NewTransform(database, rp, mst string) *Transform {
	return &Transform{
		Database:        database,
		RetentionPolicy: rp,
		Measurement:     mst,
		Columns:         make(map[string]*Column),
	}
}

// AppendLine writes data by row with improved error handling
func (t *Transform) AppendLine(tags map[string]string, fields map[string]interface{}, timestamp int64) error {
	t.mux.Lock()
	defer t.mux.Unlock()

	// process tags
	if err := t.processTagColumns(tags); err != nil {
		return err
	}

	// process fields
	if err := t.processFieldColumns(fields); err != nil {
		return err
	}

	// process timestamp
	if err := t.processTimestamp(timestamp); err != nil {
		return err
	}

	t.RowCount++

	// fill another field or tag
	if err := t.processMissValueColumns(); err != nil {
		return err
	}

	return nil
}

func (t *Transform) createColumn(name string, fieldType int) (*Column, error) {
	column := &Column{
		schema: record.Field{
			Type: fieldType,
			Name: name,
		},
		cv: record.ColVal{},
	}
	column.cv.Init()
	if err := t.appendFieldNulls(column, t.RowCount); err != nil {
		return nil, err
	}

	return column, nil
}

func (t *Transform) appendFieldNulls(column *Column, count int) error {
	switch column.schema.Type {
	case influx.Field_Type_Tag, influx.Field_Type_String:
		column.cv.AppendStringNulls(count)
		return nil
	case influx.Field_Type_Int, influx.Field_Type_UInt:
		column.cv.AppendIntegerNulls(count)
		return nil
	case influx.Field_Type_Boolean:
		column.cv.AppendBooleanNulls(count)
		return nil
	case influx.Field_Type_Float:
		column.cv.AppendFloatNulls(count)
		return nil
	default:
		return ErrInvalidFieldType
	}
}

// getFieldType returns the corresponding Field type based on the field value
func (t *Transform) getFieldType(v interface{}) (int, error) {
	switch v.(type) {
	case string:
		return influx.Field_Type_String, nil
	case bool:
		return influx.Field_Type_Boolean, nil
	case float64, float32:
		return influx.Field_Type_Float, nil
	case int, int64, int32, uint, uint32, uint64:
		return influx.Field_Type_Int, nil
	}
	return influx.Field_Type_Unknown, ErrUnknownFieldType
}

// appendFieldValue appends field value to the column
func (t *Transform) appendFieldValue(column *Column, value interface{}) error {
	switch v := value.(type) {
	case string:
		column.cv.AppendString(v)
	case bool:
		column.cv.AppendBoolean(v)
	case float64:
		column.cv.AppendFloat(v)
	case float32:
		column.cv.AppendFloat(float64(v))
	case int:
		column.cv.AppendInteger(int64(v))
	case int64:
		column.cv.AppendInteger(v)
	case int32:
		column.cv.AppendInteger(int64(v))
	case uint:
		column.cv.AppendInteger(int64(v))
	case uint32:
		column.cv.AppendInteger(int64(v))
	case uint64:
		column.cv.AppendInteger(int64(v))
	}
	// For unknown types, try to throw error
	return ErrUnknownFieldType
}

func (t *Transform) processTagColumns(tags map[string]string) (err error) {
	for tagName, tagValue := range tags {
		if err := validateName(tagName); err != nil {
			return err
		}
		tagColumn, ok := t.Columns[tagName]
		if !ok {
			tagColumn, err = t.createColumn(tagName, influx.Field_Type_String)
			if err != nil {
				return err
			}
		}
		// write the tag value to column
		tagColumn.cv.AppendString(tagValue)
		t.fillChecker[tagName] = true
		t.Columns[tagName] = tagColumn
	}
	return nil
}

func (t *Transform) processFieldColumns(fields map[string]interface{}) (err error) {
	for fieldName, fieldValue := range fields {
		if err := validateName(fieldName); err != nil {
			return err
		}
		fieldType, err := t.getFieldType(fieldValue)
		if err != nil {
			return err
		}
		fieldColumn, ok := t.Columns[fieldName]
		if !ok {
			fieldColumn, err = t.createColumn(fieldName, fieldType)
			if err != nil {
				return err
			}
		}

		if err := t.appendFieldValue(fieldColumn, fieldValue); err != nil {
			return err
		}

		t.fillChecker[fieldName] = true
		t.Columns[fieldName] = fieldColumn
	}
	return nil
}

// processTimestamp handles timestamp processing with validation
func (t *Transform) processTimestamp(timestamp int64) (err error) {
	if timestamp == 0 {
		timestamp = time.Now().UnixNano()
	}

	timeCol, exists := t.Columns[TimeColumnName]
	if !exists {
		timeCol, err = t.createColumn(TimeColumnName, influx.Field_Type_Int)
		if err != nil {
			return err
		}
	}

	timeCol.cv.AppendInteger(timestamp)
	t.Columns[TimeColumnName] = timeCol

	// Update min/max time
	if timestamp < t.MinTime {
		t.MinTime = timestamp
	}
	if timestamp > t.MaxTime {
		t.MaxTime = timestamp
	}
	return nil
}

func (t *Transform) processMissValueColumns() error {
	for fieldName, ok := range t.fillChecker {
		if ok {
			continue
		}
		column, ok := t.Columns[fieldName]
		if !ok {
			continue
		}
		offset := column.cv.Len - t.RowCount
		if offset == 0 {
			continue
		}
		if err := t.appendFieldNulls(column, offset); err != nil {
			return err
		}
	}
	t.resetFillChecker()
	return nil
}

// validateName checks if the column name is valid
func validateName(name string) error {
	if name == "" {
		return ErrEmptyName
	}
	if name == TimeColumnName {
		return ErrInvalidTimeColumn
	}
	return nil
}

// ToSrvRecords converts to record.Record with improved sorting and validation
func (t *Transform) ToSrvRecords() (*record.Record, error) {
	t.mux.RLock()
	defer t.mux.RUnlock()

	if len(t.Columns) == 0 {
		return nil, errors.New("no columns to convert")
	}

	rec := &record.Record{}
	rec.Schema = make([]record.Field, 0, len(t.Columns))
	rec.ColVals = make([]record.ColVal, 0, len(t.Columns))

	for _, column := range t.Columns {
		rec.Schema = append(rec.Schema, column.schema)
		rec.ColVals = append(rec.ColVals, column.cv)
	}

	// Sort and validate the record
	sort.Sort(rec)
	rec = record.NewColumnSortHelper().Sort(rec)
	record.CheckRecord(rec)

	return rec, nil
}

// resetFillChecker clears fill checker map
func (t *Transform) resetFillChecker() {
	for key := range t.fillChecker {
		t.fillChecker[key] = false
	}
}
