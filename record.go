package main

import "github.com/openGemini/openGemini/lib/record"

type Column struct {
	schema record.Field
	colVal record.ColVal
}

type Writer struct {
	Measurement string
	RowCount    int64
	MinTime     int64
	MaxTime     int64
	// 客户端传入一个point进来 遍历point的所有列 将其值追加到对应的column的values中
	Columns map[string]*Column
}

// AppendLine 按行写入 遍历所有字段 将其值都追加进入Columns中
func (r *Writer) AppendLine(tags map[string]string, fields map[string]interface{}) {

}

// AppendRows 按列写入 注意字段补齐
func (r *Writer) AppendRows(cols map[string][]interface{}) {

}

// ToSrvRecords 转换成服务端所需的结构record.Record
func (r *Writer) ToSrvRecords() *record.Record {
	return nil
}
