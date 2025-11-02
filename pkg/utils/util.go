package utils

import (
	"runtime"
	"strings"

	"github.com/google/uuid"
)

// GenReqId generates a UUID to be associated with the request
func GenReqId() string {
	rqId, _ := uuid.NewRandom()
	rqIdStr := rqId.String()
	// Return last 12 characters of UUID string
	return rqIdStr[len(rqIdStr)-12:]
}

// FunctName generates the function name to be used during emitting metric
func FunctName() string {
	pc := make([]uintptr, 1)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	// Extract only the function name without the package path
	return frame.Function[strings.LastIndex(frame.Function, ".")+1:]
}
