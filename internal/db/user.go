package db

/**
 * @Description: 用户信息表
 */

import (
	"context"
	"time"
)

type User struct {
	UserName string
	UserPwd  string
	SignupAt time.Time
}

// 用户注册
func UserSingup(ctx context.Context, username, userpwd string) error {
	_, err := DB.ExecContext(ctx,
		"INSERT INTO tbl_user (user_name, user_pwd) VALUES (?, ?)",
		username, userpwd)
	return err
}

// 根据用户名查询用户信息
func GetUserByNameWithPwd(ctx context.Context, username string) (*User, error) {
	u := &User{}
	err := DB.QueryRowContext(ctx,
		"SELECT user_name, user_pwd FROM tbl_user WHERE user_name = ? LIMIT 1",
		username).Scan(&u.UserName, &u.UserPwd)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// 根据用户名查询用户信息
func GetUserInfo(ctx context.Context, username string) (*User, error) {
	u := &User{}

	err := DB.QueryRowContext(ctx,
		`SELECT user_name, signup_at FROM tbl_user WHERE user_name = ? LIMIT 1`,
		username,
	).Scan(&u.UserName, &u.SignupAt)
	if err != nil {
		return nil, err
	}
	return u, nil
}
