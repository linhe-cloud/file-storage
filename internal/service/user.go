package service

import (
	"context"
	"errors"
	"file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/handler/auth"
	"time"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrUserExists         = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrInvalidUserInput   = errors.New("invalid user input")
)

type UserService struct{}

func NewUserService() *UserService { return &UserService{} }

func (s *UserService) Signup(ctx context.Context, username, password string) error {
	if len(username) < 3 || len(username) > 20 || len(password) < 6 {
		return ErrInvalidUserInput
	}
	if existing, err := db.GetUserByNameWithPwd(ctx, username); err == nil && existing != nil {
		return ErrUserExists
	}
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return db.UserSingup(ctx, username, string(hashed))
}

func (s *UserService) Signin(ctx context.Context, username, password string) (string, error) {
	if username == "" || password == "" {
		return "", ErrInvalidCredentials
	}
	u, err := db.GetUserByNameWithPwd(ctx, username)
	if err != nil || u == nil || u.Status != 0 || !verifyPassword(u.UserPwd, password) {
		return "", ErrInvalidCredentials
	}
	ttl := 24 * time.Hour
	token, err := auth.GenerateToken(username, ttl)
	if err != nil {
		return "", err
	}
	if err := redis.SetUserToken(ctx, username, token, ttl); err != nil {
		return "", err
	}
	return token, nil
}

func (s *UserService) Info(ctx context.Context, username string) (*redis.User, error) {
	return redis.GetUserInfoCache(ctx, username, func(ctx context.Context, username string) (*redis.User, error) {
		u, err := db.GetUserInfo(ctx, username)
		if err != nil {
			return nil, err
		}
		return &redis.User{UserName: u.UserName, SignupAt: u.SignupAt}, nil
	})
}

func (s *UserService) Signout(ctx context.Context, username string) error {
	return redis.DeleteUserToken(ctx, username)
}

func verifyPassword(hashedPassword, rawPassword string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(rawPassword)) == nil
}
