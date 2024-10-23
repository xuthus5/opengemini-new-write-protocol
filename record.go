package main

type Point struct {
}

// Field 字段类型
type Field int

const (
	Field_Type_Unknown Field = iota
	Field_Type_Int
	Field_Type_UInt
	Field_Type_Float
	Field_Type_String
	Field_Type_Boolean
	Field_Type_Tag
	Field_Type_Last
)

type ColVal struct {
	Val          []byte
	Offset       []uint32
	Bitmap       []byte
	BitMapOffset int
	Len          int
	NilCount     int
}

// Column 一个列所包涵的信息
type Column struct {
	schema Field    // 字段类型信息
	values []ColVal // 字段值列表
}

type Records struct {
	Measurement string
	RowCount    int64
	MinTime     int64
	MaxTime     int64
	// 客户端传入一个point进来 遍历point的所有列 将其值追加到对应的column的values中
	Columns map[string]*Column
}

// Append 遍历point所有字段 将其值都追加进入Columns中
func (r *Records) Append(point Point) {
	// tag1 := r.Columns["tag1"].values
	// tag1 = append(tag1, point.Tag1)
}

// ToSrvRecords 转换成服务端所需的结构record.Record
func (r *Records) ToSrvRecords() {}
