package ipmc

import (
	"strconv"
	"strings"
	"time"
)

func deescapeString(str string) string {
	return strings.ReplaceAll(str, "\\n", "\n")
}

func Timestamp() string {
	currentTime := time.Now()
	unixTimestamp := currentTime.Unix()
	return strconv.FormatInt(unixTimestamp, 10)
}
