package auth

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"file-storage-linhe/config"
	"file-storage-linhe/internal/cache/redis"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

type ctxKey string

const ctxKeyUsername ctxKey = "username"

var (
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
)

func init() {
	privateKeyPEM, err := readKeyFile(config.JWTPrivateKeyPath)
	if err != nil {
		panic("failed to read JWT private key: " + err.Error())
	}
	publicKeyPEM, err := readKeyFile(config.JWTPublicKeyPath)
	if err != nil {
		panic("failed to read JWT public key: " + err.Error())
	}
	privateKey = parsePrivateKey(privateKeyPEM)
	publicKey = parsePublicKey(publicKeyPEM)
}

func readKeyFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err == nil || filepath.IsAbs(path) {
		return data, err
	}
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, err
	}
	rootRelativePath := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../../", path))
	return os.ReadFile(rootRelativePath)
}

func parsePrivateKey(data []byte) *rsa.PrivateKey {
	block, _ := pem.Decode(data)
	if block == nil {
		panic("failed to parse PEM block containing private key")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		key, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			panic("parsed private key is not RSA")
		}
		return key
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		panic("failed to parse private key: " + err.Error())
	}
	return key
}

func parsePublicKey(data []byte) *rsa.PublicKey {
	block, _ := pem.Decode(data)
	if block == nil {
		panic("failed to parse PEM block containing public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic("failed to parse public key: " + err.Error())
	}
	key, ok := parsed.(*rsa.PublicKey)
	if !ok {
		panic("parsed public key is not RSA")
	}
	return key
}

type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

func GenerateToken(username string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Subject:   username,
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(privateKey)
}

func ParseToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodRS256 {
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

func Auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if header == "" || !strings.HasPrefix(header, "Bearer ") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		tokenString := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
		if tokenString == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		claims, err := ParseToken(tokenString)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		storedToken, err := redis.GetUserToken(r.Context(), claims.Username)
		if err != nil || storedToken != tokenString {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyUsername, claims.Username)
		next(w, r.WithContext(ctx))
	}
}

func UsernameFromContext(ctx context.Context) (string, bool) {
	name, ok := ctx.Value(ctxKeyUsername).(string)
	return name, ok
}
