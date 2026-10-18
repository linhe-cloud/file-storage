package transform

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/parquet-go/parquet-go"
)

func ConvertCSVToParquet(ctx context.Context, csvReader io.Reader, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建 Parquet 文件失败: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(csvReader)

	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("读取 CSV 列头失败: %w", err)
	}

	const sampleSize = 100
	samples := make([][]string, 0, sampleSize)
	for i := 0; i < sampleSize; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取 CSV 数据失败: %w", err)
		}
		samples = append(samples, row)
	}

	colTypes := inferTypes(headers, samples)

	group := make(parquet.Group, len(headers))
	for i, header := range headers {
		switch colTypes[i] {
		case "int64":
			group[header] = parquet.Int(64)
		case "float64":
			group[header] = parquet.Leaf(parquet.DoubleType)
		default:
			group[header] = parquet.String()
		}
	}
	schema := parquet.NewSchema("csv_record", group)

	writer := parquet.NewWriter(file, schema)

	for _, row := range samples {
		parquetRow := make(parquet.Row, len(headers))
		for i, val := range row {
			if i < len(headers) {
				parquetRow[i] = convertValue(val, colTypes[i])
			}
		}
		if _, err := writer.WriteRows([]parquet.Row{parquetRow}); err != nil {
			return fmt.Errorf("写入 Parquet 行失败: %w", err)
		}
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取 CSV 数据失败: %w", err)
		}
		parquetRow := make(parquet.Row, len(headers))
		for i, val := range row {
			if i < len(headers) {
				parquetRow[i] = convertValue(val, colTypes[i])
			}
		}
		if _, err := writer.WriteRows([]parquet.Row{parquetRow}); err != nil {
			return fmt.Errorf("写入 Parquet 行失败: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("关闭 Parquet writer 失败: %w", err)
	}

	return nil
}

func inferTypes(headers []string, samples [][]string) []string {
	types := make([]string, len(headers))
	for i := range headers {
		types[i] = "int64"
	}

	for _, row := range samples {
		for i, val := range row {
			if i >= len(types) {
				break
			}
			currentType := types[i]
			if currentType == "string" {
				continue
			}
			if currentType == "int64" {
				if _, err := strconv.ParseInt(val, 10, 64); err != nil {
					types[i] = "float64"
				}
			}
			if currentType == "float64" {
				if _, err := strconv.ParseFloat(val, 64); err != nil {
					types[i] = "string"
				}
			}
		}
	}

	return types
}

func convertValue(val string, typ string) parquet.Value {
	switch typ {
	case "int64":
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parquet.ValueOf(v)
		}
		return parquet.ValueOf(int64(0))
	case "float64":
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			return parquet.ValueOf(v)
		}
		return parquet.ValueOf(float64(0))
	default:
		return parquet.ValueOf(val)
	}
}
