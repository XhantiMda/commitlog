package log_v1

import (
	"fmt"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

func (errO ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.Newf(404, fmt.Sprintf("offset out of range: %d", errO.Offset))
	msg := fmt.Sprintf("The requested offset is outside the log's range: %d", errO.Offset)

	errDtls := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}

	details, err := st.WithDetails(errDtls)
	if err != nil {
		return st
	}

	return details
}

func (errO ErrOffsetOutOfRange) Error() string {
	return errO.GRPCStatus().Err().Error()
}

type ErrOffsetOutOfRange struct {
	Offset uint64
}
