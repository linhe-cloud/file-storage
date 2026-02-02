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
