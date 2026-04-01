package util

import (
	"regexp"
	"strings"
	"syscall"
)

func EnvString(key, fallback string) string {
	if val, ok := syscall.Getenv(key); ok {
		return val
	}

	return fallback
}

func SplitComma(s string) []string {
	var regComma = regexp.MustCompile(`\s*,\s*`)
	return regComma.Split(strings.TrimSpace(s), -1)
}
