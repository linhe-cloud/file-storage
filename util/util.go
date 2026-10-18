package util

/**
 * @Description: 工具包
 */

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
)

/**
 * @Description: 生成文件的 SHA1 标识
 */

// 计算文件的 SHA1 值
func FileSha1(file *os.File) string {
	h := sha1.New()
	io.Copy(h, file)
	return hex.EncodeToString(h.Sum(nil))
}

// ComputeSha1ByReader 计算 Reader 的 SHA1 值并返回文件大小和哈希
func ComputeSha1ByReader(reader io.Reader) (int64, string, error) {
	h := sha1.New()
	size, err := io.Copy(h, reader)
	if err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}
