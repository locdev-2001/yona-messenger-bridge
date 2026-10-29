package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/bloks"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/loginerrors"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

const (
	loginTTL             = 10 * time.Minute
	loginJanitorInterval = time.Minute
	loginStepIDPrefix    = "fi.mau.meta.messengerlite."
	platformIOS          = "ios"
	platformAndroid      = "android"

	fieldTypeText     = "text"
	fieldTypeEmail    = "email"
	fieldTypePhone    = "phone"
	fieldTypePassword = "password"
	fieldTypeCode     = "code"
	fieldTypeCaptcha  = "captcha"
	fieldTypeSelect   = "select"
)

// Login steps that Meta shows again, with an error line, when the submitted
// value was wrong. A repeat of one of these after a submission is a failure.
var credentialStepIDs = map[string]bool{"email_password": true, "password": true}
var codeStepIDs = map[string]bool{"otp_code": true, "totp": true, "sms": true, "backup_code": true, "whatsapp": true}

type LoginStep struct {
	Type         string            `json:"type"`
	StepID       string            `json:"step_id"`
	Instructions *string           `json:"instructions"`
	Fields       []LoginField      `json:"fields"`
	Attachments  []LoginAttachment `json:"attachments"`
}

// LoginAttachment carries what the person must look at or listen to before
// answering a step, such as the captcha image and its audio version.
type LoginAttachment struct {
	Kind     string `json:"kind"`
	MimeType string `json:"mime_type"`
	Data     string `json:"data_base64"`
}

type LoginField struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Description *string  `json:"description"`
	Options     []string `json:"options"`
}

type loginAccount struct {
	FacebookUserID string `json:"facebook_user_id"`
	DisplayName    string `json:"display_name"`
	AvatarURL      any    `json:"avatar_url"`
}

type loginResult struct {
	step    *LoginStep
	account *loginAccount
	waiting bool
}

// approvalPollWindow bounds one wait for an approval made on another device.
// The attempt survives the window, so the caller simply asks again.
const approvalPollWindow = 40 * time.Second

// loginAttempt is one multi-screen native login, held only in memory. The
// password and codes pass straight from the request to Meta.
type loginAttempt struct {
	mu           sync.Mutex
	connectionID int64
	platform     types.Platform
	client       *messagix.Client
	createdAt    time.Time
	lastStep     *bridgev2.LoginStep
}

type LoginManager struct {
	slots      *SlotManager
	libraryLog zerolog.Logger
	log        zerolog.Logger

	mu       sync.Mutex
	attempts map[string]*loginAttempt
}

func NewLoginManager(slots *SlotManager, loggers *Loggers) *LoginManager {
	return &LoginManager{
		slots:      slots,
		libraryLog: loggers.Library.With().Str("component", "login").Logger(),
		log:        loggers.App.With().Str("component", "login").Logger(),
		attempts:   make(map[string]*loginAttempt),
	}
}

func parseLoginPlatform(name string) (types.Platform, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", platformIOS:
		return types.MessengerLiteIOS, nil
	case platformAndroid:
		return types.MessengerLiteAndroid, nil
	}
	return types.Unset, errBadRequest
}

func (manager *LoginManager) pendingNewConnections() int {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return len(manager.attempts)
}

func (manager *LoginManager) Start(ctx context.Context, token string, connectionID int64, platformName string) (*LoginStep, error) {
	platform, err := parseLoginPlatform(platformName)
	if err != nil {
		return nil, err
	}
	if !manager.slots.HasCapacityFor(connectionID, manager.pendingNewConnections()) {
		manager.log.Warn().Str("event", "login_refused_capacity").Int64("connection_id", connectionID).Send()
		return nil, errSessionCap
	}
	attempt := &loginAttempt{
		connectionID: connectionID,
		platform:     platform,
		createdAt:    time.Now(),
		client: messagix.NewClient(&cookies.Cookies{Platform: platform},
			manager.libraryLog.With().Int64("connection_id", connectionID).Logger(), &messagix.Config{}),
	}
	manager.mu.Lock()
	if _, exists := manager.attempts[token]; exists {
		manager.mu.Unlock()
		return nil, newAPIError(http.StatusBadRequest, codeBadRequest, "This login token is already in use.")
	}
	manager.attempts[token] = attempt
	manager.mu.Unlock()

	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	step, sessionCookies, err := attempt.client.MessengerLite.DoLoginSteps(ctx, nil)
	if err != nil || sessionCookies != nil {
		manager.forget(token)
		manager.log.Warn().Str("event", "login_start_failed").Int64("connection_id", connectionID).Str("code", loginErrorCode(err)).Send()
		return nil, mapLoginError(err)
	}
	attempt.lastStep = step
	manager.log.Info().Str("event", "login_started").Int64("connection_id", connectionID).Str("step_id", shortStepID(step.StepID)).Send()
	return convertLoginStep(step), nil
}

func (manager *LoginManager) Submit(ctx context.Context, token string, input map[string]string) (*loginResult, error) {
	attempt, err := manager.liveAttempt(token)
	if err != nil {
		return nil, err
	}
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if input == nil {
		input = map[string]string{}
	}
	submittedStep := attempt.lastStep
	startedAt := time.Now()
	awaitingApproval := submittedStep != nil && submittedStep.Type == bridgev2.LoginStepTypeDisplayAndWait
	if awaitingApproval {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, approvalPollWindow)
		defer cancel()
	}
	step, sessionCookies, err := attempt.client.MessengerLite.DoLoginSteps(ctx, input)
	clear(input)
	if err != nil && awaitingApproval && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
		manager.log.Info().Str("event", "login_still_waiting").Int64("connection_id", attempt.connectionID).Send()
		return &loginResult{step: convertLoginStep(submittedStep), waiting: true}, nil
	}
	if err != nil {
		manager.forget(token)
		manager.log.Warn().Str("event", "login_failed").Int64("connection_id", attempt.connectionID).
			Str("code", loginErrorCode(err)).Int64("duration_ms", time.Since(startedAt).Milliseconds()).Send()
		return nil, mapLoginError(err)
	}
	if step != nil {
		attempt.lastStep = step
		if rejection := repeatedStepRejection(submittedStep, step); rejection != nil {
			manager.log.Info().Str("event", "login_step_rejected").Int64("connection_id", attempt.connectionID).
				Str("step_id", shortStepID(step.StepID)).Str("code", rejection.code).Send()
			return nil, rejection
		}
		manager.log.Info().Str("event", "login_step").Int64("connection_id", attempt.connectionID).Str("step_id", shortStepID(step.StepID)).Send()
		return &loginResult{step: convertLoginStep(step)}, nil
	}
	manager.forget(token)
	session := newStoredSession(attempt.connectionID, attempt.platform, sessionCookies, attempt.client.MessengerLite.GetNativeSession())
	account, err := manager.slots.ActivateLogin(ctx, session)
	if err != nil {
		return nil, err
	}
	manager.log.Info().Str("event", "login_completed").Int64("connection_id", attempt.connectionID).
		Int64("duration_ms", time.Since(attempt.createdAt).Milliseconds()).Send()
	return &loginResult{account: account}, nil
}

func (manager *LoginManager) Cancel(token string) {
	manager.mu.Lock()
	attempt := manager.attempts[token]
	delete(manager.attempts, token)
	manager.mu.Unlock()
	if attempt != nil {
		manager.log.Info().Str("event", "login_cancelled").Int64("connection_id", attempt.connectionID).Send()
	}
}

func (manager *LoginManager) liveAttempt(token string) (*loginAttempt, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	attempt, exists := manager.attempts[token]
	if !exists {
		return nil, errLoginNotFound
	}
	if time.Since(attempt.createdAt) > loginTTL {
		delete(manager.attempts, token)
		return nil, errLoginExpired
	}
	return attempt, nil
}

func (manager *LoginManager) forget(token string) {
	manager.mu.Lock()
	delete(manager.attempts, token)
	manager.mu.Unlock()
}

func (manager *LoginManager) RunJanitor(ctx context.Context) {
	ticker := time.NewTicker(loginJanitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			manager.mu.Lock()
			for token, attempt := range manager.attempts {
				if time.Since(attempt.createdAt) > loginTTL {
					delete(manager.attempts, token)
				}
			}
			manager.mu.Unlock()
		}
	}
}

// repeatedStepRejection turns "Meta showed the same input screen again" into
// a contract error. The attempt stays alive so the user can retry the step.
func repeatedStepRejection(submitted, next *bridgev2.LoginStep) *apiError {
	if submitted == nil || submitted.Type != bridgev2.LoginStepTypeUserInput || submitted.StepID != next.StepID {
		return nil
	}
	stepID := shortStepID(next.StepID)
	switch {
	case credentialStepIDs[stepID]:
		return newAPIError(http.StatusUnprocessableEntity, codeInvalidCredentials, "Messenger rejected the email or password.")
	case codeStepIDs[stepID]:
		return newAPIError(http.StatusUnprocessableEntity, codeTwoFactorFailed, "Messenger rejected the code.")
	}
	return nil
}

func shortStepID(stepID string) string {
	return strings.TrimPrefix(stepID, loginStepIDPrefix)
}

func convertLoginStep(step *bridgev2.LoginStep) *LoginStep {
	converted := &LoginStep{
		Type:         string(step.Type),
		StepID:       shortStepID(step.StepID),
		Instructions: optionalString(step.Instructions),
		Fields:       []LoginField{},
		Attachments:  []LoginAttachment{},
	}
	if step.Type != bridgev2.LoginStepTypeUserInput && step.Type != bridgev2.LoginStepTypeDisplayAndWait {
		converted.Type = string(bridgev2.LoginStepTypeUserInput)
	}
	if step.UserInputParams == nil {
		return converted
	}
	for _, field := range step.UserInputParams.Fields {
		options := field.Options
		if options == nil {
			options = []string{}
		}
		converted.Fields = append(converted.Fields, LoginField{
			ID:          field.ID,
			Name:        field.Name,
			Type:        convertFieldType(field.Type),
			Description: optionalString(field.Description),
			Options:     options,
		})
	}
	for _, attachment := range step.UserInputParams.Attachments {
		if attachment == nil || len(attachment.Content) == 0 {
			continue
		}
		converted.Attachments = append(converted.Attachments, LoginAttachment{
			Kind:     attachmentKind(attachment.Info.MimeType),
			MimeType: attachment.Info.MimeType,
			Data:     base64.StdEncoding.EncodeToString(attachment.Content),
		})
	}
	return converted
}

func attachmentKind(mimeType string) string {
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return "image"
	case strings.HasPrefix(mimeType, "audio/"):
		return "audio"
	}
	return "file"
}

func convertFieldType(fieldType bridgev2.LoginInputFieldType) string {
	switch fieldType {
	case bridgev2.LoginInputFieldTypePassword:
		return fieldTypePassword
	case bridgev2.LoginInputFieldTypeEmail:
		return fieldTypeEmail
	case bridgev2.LoginInputFieldTypePhoneNumber:
		return fieldTypePhone
	case bridgev2.LoginInputFieldType2FACode:
		return fieldTypeCode
	case bridgev2.LoginInputFieldTypeCaptchaCode:
		return fieldTypeCaptcha
	case bridgev2.LoginInputFieldTypeSelect:
		return fieldTypeSelect
	}
	return fieldTypeText
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func loginErrorCode(err error) string {
	if err == nil {
		return codeRejected
	}
	return mapLoginError(err).code
}

// mapLoginError translates library login failures into contract codes. The
// library text is never passed through: it may echo what the user typed.
func mapLoginError(err error) *apiError {
	var respErr bridgev2.RespError
	if errors.As(err, &respErr) {
		switch respErr.ErrCode {
		case loginerrors.RateLimited.ErrCode:
			return newAPIError(http.StatusTooManyRequests, codeRateLimited, "Messenger is rate limiting logins, try again later.")
		case loginerrors.Checkpoint.ErrCode, loginerrors.Challenge.ErrCode, loginerrors.Consent.ErrCode,
			loginerrors.AccountSuspended.ErrCode, "FI.MAU.META_ACCOUNT_RECOVERY_REQUIRED":
			return newAPIError(http.StatusUnprocessableEntity, codeCheckpoint, "Messenger requires a security check in the official app.")
		case loginerrors.AFADStopped.ErrCode, loginerrors.NoSupportedMFA.ErrCode, loginerrors.MandatoryOAuth.ErrCode,
			loginerrors.MandatoryPasskey.ErrCode, loginerrors.ReCaptcha.ErrCode, loginerrors.NoSMSAvailable.ErrCode:
			return newAPIError(http.StatusUnprocessableEntity, codeTwoFactorFailed, "Two-factor verification could not be completed.")
		case loginerrors.PhoneNumber.ErrCode, loginerrors.InvalidUsername.ErrCode:
			return newAPIError(http.StatusUnprocessableEntity, codeInvalidCredentials, "Use the email address or username of the account.")
		}
		return errMetaRejected
	}
	var checkpointErr bloks.CheckpointError
	if errors.As(err, &checkpointErr) {
		return newAPIError(http.StatusUnprocessableEntity, codeCheckpoint, "Messenger requires a security check in the official app.")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return newAPIError(http.StatusGatewayTimeout, codeUnknown, "Messenger did not answer in time.")
	}
	var typed *apiError
	if errors.As(err, &typed) {
		return typed
	}
	return errMetaRejected
}
