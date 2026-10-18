package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

func TestTokenRoundTrip(t *testing.T) {
	token, err := GenerateToken("test-user", time.Minute)
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}
	claims, err := ParseToken(token)
	if err != nil {
		t.Fatalf("ParseToken() error = %v", err)
	}
	if claims.Username != "test-user" {
		t.Fatalf("username = %q, want %q", claims.Username, "test-user")
	}
}

func TestParseTokenRejectsNonRS256(t *testing.T) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		Username: "test-user",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		},
	})
	raw, err := token.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	if _, err := ParseToken(raw); err == nil {
		t.Fatal("ParseToken accepted a non-RS256 token")
	}
}
