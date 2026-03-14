package handler

import (
	"bytes"
	"fmt"
	"text/template"
)

// renderTemplate renders body as a Go template with vars and returns the result (variable substitution).
func renderTemplate(body string, vars map[string]string) (string, error) {
	if body == "" {
		return "", nil
	}
	t, err := template.New("").Parse(body)
	if err != nil {
		return "", fmt.Errorf("invalid template: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("template render: %w", err)
	}
	return buf.String(), nil
}
