package auth

/**
 * @Description: JWT认证授权
 */

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"file-storage-linhe/internal/cache/redis"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

type ctxKey string

const ctxKeyUsername ctxKey = "username"

const privateKeyPEM = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDSQxRiXuXE1cV+
bvBZfhnQPjVaDimpXNMH1d97mp7nxTey03BDbGAQrQozYtiNCptcJ8CqW6dTwM0H
FX8D3f1TaIx5AFF1TDzwMsxpswYybuVK2ANxL82ObWKaHt6cvsrlIcc3XSv4aFzX
X0jWXvX+AuuqQkcqM18JmDbpBkYDdjKDZA0cT23XrRkfzlLg8bWfkj7cFT2/Q185
ox2AkVSHdFZTY4ix8YvBLAqFd4IXGeQg3Qq+WHQkyrEJx5iLUcvBP+r5R+4+exVw
Ak6m0sEoltu9eSscmcnGcD5nMHIDh0zuWkxb2y25jGGuxVdo81HgB0ouKrvl9Vca
IONy7qRVAgMBAAECggEAO2Zk81dw3PcxeIL51qZ3v2tboinfHjARlCSZIq+vVbXT
AuuVyw5qYzunZpX1rH7DHjlCxj3nWxNcOLSbcem8X9xo1uQ+nv8fNhJ7yh6f0Q6g
K1E0Q70n+qUvVnADHtANvMaao6roOaCuHdAW4SzpnM2Ra0OXHDZwEmNUm+dAT0iJ
OSQVHDeD8PO12xWveYopyJ64B/oMdj3lz0UORQLxqJ7AH01vKpMlcqFnI9VPTEnl
jUhnWfIOQD5F3z6pHgsWVkfUL5RtZlBQuKuSsN2kxEMAW44k1NLceiclqXep8pT6
rbbGNKwMKLGR6Z2sKF/yDg1vJvpkDKsBO5D7Va5lrwKBgQD1S5G08qY7NQb7gLv2
kEuVJggAXLj2M5h9jXzzdgsda3XOkFXJvzqZBx/1QHUWz3Nvrm4hPXEIkCKePtbb
0ceVta0wK0V5VnmvISUXwgU4KPimsRlnSUCI6Dv61OehlebzBKogQj/eIzdU4DpZ
P5h+8t/G7NXbScf+Y1/QAu4LHwKBgQDbcB74W7S9vESqaSVWXAGDDq9XApPdZ88i
nZWN0p5nO7xC1Wa+amdcEF1Du2BNdemkePzkj/2TRhynd6HbJ7lz5SxcqjP4sCX4
uO1N1yv2/lzqGu8TksDOeh2eLZzJZpUgg6jNt7jK2gMhaA5YdUcEiaYDW04S+V7Q
YrXyUdCWCwKBgQCbYXU49AQMuThlFFT8iMb4AZFL+XBItMJBteCSsBG2Xx3O7WsK
UxIhYXwC9QO2oU50TkAA2lTBodvr9c5QjIAq+1xriN2HURnfr9U5SXPY8Usg/Eol
PSeeEc38w+S+XKBhDHFf4ddnNVOVdjw/0KMet2a9McT+FayUHgrRTynO7QKBgCPs
ihVspf092hvv1L37l//Foqzi7iQb/4wSMk6igW4zGFH0r7pSnxMyVqJlBZII0LQt
XnFopUG8A6ri6et/6Ftx1D5jWtjGtaCvXaIVrMvlvn5Q5HzW1Ju44CpqGzXZhmZp
R3RONIF5CXQpndebc4HpqvT3uKNgkcYZsuG1xSkbAoGACqd2/AXQ7XM/xek74PPY
Jqt37Dj76XJwezeIhgOyjJ+riM8IL2V2hWuO/E91nF0XSvO+ULyaKRIq/gKLdgu2
Il1F1YfvS6DR8Iv605zVXImO1t2Drib4hsScBXqXlhWJJQ6YvyvnKsQ9XMnqsU+7
XiPh2oDIubD8eozGJMSt9WQ=
-----END PRIVATE KEY-----`

const publicKeyPEM = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0kMUYl7lxNXFfm7wWX4Z
0D41Wg4pqVzTB9Xfe5qe58U3stNwQ2xgEK0KM2LYjQqbXCfAqlunU8DNBxV/A939
U2iMeQBRdUw88DLMabMGMm7lStgDcS/Njm1imh7enL7K5SHHN10r+Ghc119I1l71
/gLrqkJHKjNfCZg26QZGA3Yyg2QNHE9t160ZH85S4PG1n5I+3BU9v0NfOaMdgJFU
h3RWU2OIsfGLwSwKhXeCFxnkIN0Kvlh0JMqxCceYi1HLwT/q+UfuPnsVcAJOptLB
KJbbvXkrHJnJxnA+ZzByA4dM7lpMW9stuYxhrsVXaPNR4AdKLiq75fVXGiDjcu6k
VQIDAQAB
-----END PUBLIC KEY-----`

var (
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
)

// 初始化 init 解析 PEM
func init() {
	// 解析私钥
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		panic("failed to parse PEM block containing private key")
	}

	// 尝试 PKCS8 格式（BEGIN PRIVATE KEY）
	parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// 如果 PKCS8 失败，尝试 PKCS1 格式（BEGIN RSA PRIVATE KEY）
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			panic("failed to parse private key: " + err.Error())
		}
		privateKey = key
	} else {
		// PKCS8 解析成功，转换为 RSA 私钥
		rsaKey, ok := parsedKey.(*rsa.PrivateKey)
		if !ok {
			panic("parsed key is not *rsa.PrivateKey")
		}
		privateKey = rsaKey
	}

	// 解析公钥
	pubBlock, _ := pem.Decode([]byte(publicKeyPEM))
	if pubBlock == nil {
		panic("failed to parse PEM block containing public key")
	}
	pubAny, err := x509.ParsePKIXPublicKey(pubBlock.Bytes)
	if err != nil {
		panic("failed to parse public key: " + err.Error())
	}
	rsaPub, ok := pubAny.(*rsa.PublicKey)
	if !ok {
		panic("public key is not an RSA public key")
	}
	publicKey = rsaPub
}

// 自定义 Claims
type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// 生成 JWT：登陆成功后调用（使用私钥 RS256 签名）
func GenerateToken(username string, tll time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(tll)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Subject:   username,
			// 可根据需求添加签发者
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(privateKey)
}

// 解析 + 校验 JWT（使用公钥验证签名）
func ParseToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return publicKey, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}

// 鉴权中间件：从 Authorization: Bearer <token> 中取出 JWT 验证
func Auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. 读取 Authorization 头
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// 2. 去掉 "Bearer " 前缀
		tokenStr := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		if tokenStr == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// 3. 解析并校验 JWT
		claims, err := ParseToken(tokenStr)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// 4. 单点登录校验：检查redis中token是否与当前一致
		storedToken, err := redis.GetUserToken(r.Context(), claims.Username)
		if err != nil || storedToken != tokenStr {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// 5. 把用户名放进 context，交给后面的业务 handler 用
		ctx := context.WithValue(r.Context(), ctxKeyUsername, claims.Username)
		next(w, r.WithContext(ctx))
	}
}

// 业务 handler 想拿当前登录用户时调用
func UsernameFromContext(ctx context.Context) (string, bool) {
	v := ctx.Value(ctxKeyUsername)
	name, ok := v.(string)
	return name, ok
}
