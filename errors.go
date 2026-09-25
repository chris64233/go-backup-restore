package backuprestore

import (
	"errors"
	"fmt"
)

// ErrorCode 对错误进行分类，调用方可据此决定是否重试或如何上报。
type ErrorCode string

const (
	ErrCodeInvalidArgument ErrorCode = "invalid_argument" // 输入不合法，重试无意义
	ErrCodeNotFound        ErrorCode = "not_found"        // 引用的对象不存在
	ErrCodeConflict        ErrorCode = "conflict"         // 状态/并发冲突（终态、版本、租约等）
	ErrCodeAlreadyExists   ErrorCode = "already_exists"   // 唯一约束冲突
	ErrCodeLease           ErrorCode = "lease"            // 租约 epoch 不匹配（接管/回执失效）
	ErrCodeUnavailable     ErrorCode = "unavailable"      // 底层存储故障，可重试
)

// Error 携带分类码与可读信息，支持 errors.Is/As。
type Error struct {
	Code    ErrorCode
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

func classified(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func wrapErr(code ErrorCode, err error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Err: err}
}

// CodeOf 提取错误的分类码；非本包错误归为 ErrCodeUnavailable；nil 返回空串。
func CodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ErrCodeUnavailable
}
