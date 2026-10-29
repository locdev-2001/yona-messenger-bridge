package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	chatKindDirect = "direct"
	chatKindGroup  = "group"

	sourcePrefixEncrypted = "mpe:"
	sourcePrefixRegular   = "mpr:"

	codeBadRequest         = "bad_request"
	codeUnauthorized       = "unauthorized"
	codeNotFound           = "not_found"
	codeSessionLost        = "session_lost"
	codeSessionCapReached  = "session_cap_reached"
	codeRejected           = "rejected"
	codeMediaFetchFailed   = "media_fetch_failed"
	codeUnknown            = "unknown"
	codeInvalidCredentials = "invalid_credentials"
	codeTwoFactorFailed    = "two_factor_failed"
	codeCheckpoint         = "checkpoint"
	codeLoginExpired       = "login_expired"
	codeRateLimited        = "rate_limited"
)

// apiError is the only error shape the HTTP layer returns. Its message is a
// short English sentence and never contains user data.
type apiError struct {
	status  int
	code    string
	message string
}

func (err *apiError) Error() string { return err.code + ": " + err.message }

func newAPIError(status int, code, message string) *apiError {
	return &apiError{status: status, code: code, message: message}
}

var (
	errBadRequest     = newAPIError(http.StatusBadRequest, codeBadRequest, "The request is invalid.")
	errSlotNotFound   = newAPIError(http.StatusNotFound, codeNotFound, "No session exists for this connection.")
	errLoginNotFound  = newAPIError(http.StatusNotFound, codeNotFound, "No login is in progress for this token.")
	errSlotNotReady   = newAPIError(http.StatusConflict, codeSessionLost, "The session is not connected.")
	errSessionCap     = newAPIError(http.StatusTooManyRequests, codeSessionCapReached, "The bridge has reached its session limit.")
	errLoginExpired   = newAPIError(http.StatusGone, codeLoginExpired, "The login expired, start again.")
	errMetaRejected   = newAPIError(http.StatusBadGateway, codeRejected, "Messenger rejected the request.")
	errMediaFetch     = newAPIError(http.StatusBadGateway, codeMediaFetchFailed, "The media could not be fetched.")
	errUnknownFailure = newAPIError(http.StatusInternalServerError, codeUnknown, "The bridge failed to handle the request.")
)

func asAPIError(err error) *apiError {
	var typed *apiError
	if errors.As(err, &typed) {
		return typed
	}
	return errUnknownFailure
}

type Chat struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func (chat Chat) validate() error {
	if chat.Kind != chatKindDirect && chat.Kind != chatKindGroup {
		return errBadRequest
	}
	if _, err := strconv.ParseInt(chat.ID, 10, 64); err != nil {
		return errBadRequest
	}
	return nil
}

func (chat Chat) numericID() int64 {
	id, _ := strconv.ParseInt(chat.ID, 10, 64)
	return id
}

func directChat(fbid int64) Chat { return Chat{Kind: chatKindDirect, ID: formatFBID(fbid)} }
func groupChat(threadKey int64) Chat {
	return Chat{Kind: chatKindGroup, ID: formatFBID(threadKey)}
}

func formatFBID(id int64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatInt(id, 10)
}

func encryptedSourceID(stanzaID string) string { return sourcePrefixEncrypted + stanzaID }
func regularSourceID(messageID string) string  { return sourcePrefixRegular + messageID }

type parsedSourceID struct {
	encrypted bool
	id        string
}

func parseSourceID(sourceID string) (parsedSourceID, error) {
	switch {
	case strings.HasPrefix(sourceID, sourcePrefixEncrypted) && len(sourceID) > len(sourcePrefixEncrypted):
		return parsedSourceID{encrypted: true, id: strings.TrimPrefix(sourceID, sourcePrefixEncrypted)}, nil
	case strings.HasPrefix(sourceID, sourcePrefixRegular) && len(sourceID) > len(sourcePrefixRegular):
		return parsedSourceID{encrypted: false, id: strings.TrimPrefix(sourceID, sourcePrefixRegular)}, nil
	}
	return parsedSourceID{}, fmt.Errorf("%w: source id", errBadRequest)
}
