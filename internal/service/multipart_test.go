package service

import "testing"

func TestCalculateChunkSize(t *testing.T) {
	tests := []struct {
		name string
		size int64
		want int64
	}{
		{name: "small", size: 1, want: 1},
		{name: "twenty megabytes", size: 20 * 1024 * 1024, want: 20 * 1024 * 1024},
		{name: "over twenty megabytes", size: 20*1024*1024 + 1, want: 5 * 1024 * 1024},
		{name: "one hundred megabytes", size: 100 * 1024 * 1024, want: 5 * 1024 * 1024},
		{name: "over one hundred megabytes", size: 100*1024*1024 + 1, want: 10 * 1024 * 1024},
		{name: "one gigabyte", size: 1024 * 1024 * 1024, want: 10 * 1024 * 1024},
		{name: "over one gigabyte", size: 1024*1024*1024 + 1, want: 50 * 1024 * 1024},
		{name: "ten gigabytes", size: 10 * 1024 * 1024 * 1024, want: 50 * 1024 * 1024},
		{name: "over ten gigabytes", size: 10*1024*1024*1024 + 1, want: 100 * 1024 * 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateChunkSize(tt.size); got != tt.want {
				t.Fatalf("CalculateChunkSize(%d) = %d, want %d", tt.size, got, tt.want)
			}
		})
	}
}

func TestMultipartServiceInitRejectsInvalidParameters(t *testing.T) {
	svc := NewMultipartService()
	for _, tt := range []struct {
		name     string
		username string
		hash     string
		filename string
		size     int64
	}{
		{name: "missing username", hash: "hash", filename: "file.txt", size: 1},
		{name: "missing hash", username: "user", filename: "file.txt", size: 1},
		{name: "missing filename", username: "user", hash: "hash", size: 1},
		{name: "zero size", username: "user", hash: "hash", filename: "file.txt"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := svc.Init(t.Context(), tt.username, tt.hash, tt.filename, tt.size); err == nil {
				t.Fatal("Init accepted invalid parameters")
			}
		})
	}
}
