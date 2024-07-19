package utils

import (
	"log/slog"
)

func Err(err error) slog.Attr {
	return slog.Attr{
		Key:   "error",
		Value: slog.StringValue(err.Error()),
	}
}

func StarMiddleDigits(phone string) string {
	if len(phone) < 7 {
		return phone // Not enough digits to star out
	}
	return phone[:5] + "****" + phone[len(phone)-2:]
}
