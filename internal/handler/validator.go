package handler

import (
	"regexp"
	"strings"
)

var e164Regex = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)
var emailRegex = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

func validate(req CreateNotificationRequest) []FieldError {
	var errs []FieldError

	switch strings.ToLower(req.Channel) {
	case "sms":
		if !e164Regex.MatchString(req.Recipient) {
			errs = append(errs, FieldError{Field: "recipient", Message: "must be a valid E.164 phone number"})
		}
		if len(req.Content) == 0 || len(req.Content) > 160 {
			errs = append(errs, FieldError{Field: "content", Message: "must be between 1 and 160 characters"})
		}
	case "email":
		if !emailRegex.MatchString(req.Recipient) {
			errs = append(errs, FieldError{Field: "recipient", Message: "must be a valid email address"})
		}
		if len(req.Content) == 0 || len(req.Content) > 1000 {
			errs = append(errs, FieldError{Field: "content", Message: "must be between 1 and 1000 characters"})
		}
	case "push":
		if req.Recipient == "" {
			errs = append(errs, FieldError{Field: "recipient", Message: "device token is required"})
		}
		if len(req.Content) == 0 || len(req.Content) > 256 {
			errs = append(errs, FieldError{Field: "content", Message: "must be between 1 and 256 characters"})
		}
	default:
		errs = append(errs, FieldError{Field: "channel", Message: "must be sms, email or push"})
	}

	if req.Priority != "" {
		switch strings.ToLower(req.Priority) {
		case "high", "normal", "low":
		default:
			errs = append(errs, FieldError{Field: "priority", Message: "must be high, normal or low"})
		}
	}

	return errs
}
