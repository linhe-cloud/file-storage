package handler

import (
	"encoding/json"
	"file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/handler/auth"
	"net/http"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type User struct {
	ID         int64     `json:"id,omitempty"`
	Username   string    `json:"username"`
	SignupAt   time.Time `json:"signup_at,omitempty"`
	LastActive time.Time `json:"last_active,omitempty"`
}

// 设置 bcrypt 密码哈希（自动处理盐值，cost 默认为 10）
func encryptPassword(raw string) (string, error) {
	hashedBytes, err := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedBytes), nil
}

// 验证明文密码和哈希密码是否匹配
func verifyPassword(hashedPassword, rawPassword string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(rawPassword))
	return err == nil // 无错误表示匹配成功
}

// 统一 JSON 响应
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Add("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// 注册：POST /user/signup
func SignupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")
	if username == "" || password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username or password is missing"})
		return
	}

	// 校验用户名长度（3-20个字符）
	if len(username) < 3 || len(username) > 20 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username must be between 3 and 20 characters"})
		return
	}

	// 校验密码长度（至少6个字符）
	if len(password) < 6 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "password must be at least 6 characters"})
		return
	}

	// 检查用户名是否已存在
	existingUser, _ := db.GetUserByNameWithPwd(r.Context(), username)
	if existingUser != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "username already exists"})
		return
	}

	// 通过 bcrypt 加密
	hashed, err := encryptPassword(password)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to encrypt password"})
		return
	}

	// 写入数据库
	if err := db.UserSingup(r.Context(), username, hashed); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to signup"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"result": "OK"})
}

// 登录： POST /user/signin
func SigninHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")
	if username == "" || password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username or password is missing"})
		return
	}

	u, err := db.GetUserByNameWithPwd(r.Context(), username)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid username or password"})
		return
	}

	// 校验密码
	if !verifyPassword(u.UserPwd, password) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid username or password"})
		return
	}

	// 生成 JWT（24小时过期）
	tokenTTL := 24 * time.Hour
	token, err := auth.GenerateToken(username, tokenTTL)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "generate token failed"})
		return
	}

	// 存储到redis（实现单点登录，新token会覆盖旧token）
	if err := redis.SetUserToken(r.Context(), username, token, tokenTTL); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to set user token"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"username": username,
		"token":    token,
	})
}

// 获取当前登录用户信息：GET /user/info
func UserInfoHandler(w http.ResponseWriter, r *http.Request) {
	// 从 JWT 解析当前用户名
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// 查询用户信息
	u, err := db.GetUserInfo(r.Context(), username)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "get user info failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"username":  u.UserName,
		"signup_at": u.SignupAt,
	})
}

// 登出：POST /user/signout
func SignoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if err := redis.DeleteUserToken(r.Context(), username); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "signout failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"result": "OK"})
}

// 获取在线设备数：GET /user/online-devices
func OnlineDevicesHandler(w http.ResponseWriter, r *http.Request) {
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	count, err := redis.GetOnlineDeviceCount(r.Context(), username)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to get online device count"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"username":      username,
		"device_count":  count,
		"online_status": count > 0,
	})
}
