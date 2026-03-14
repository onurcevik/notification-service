package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_renderTemplate(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		vars    map[string]string
		want    string
		wantErr bool
	}{
		{
			name: "success",
			body: "Hello {{.name}}, code: {{.code}}",
			vars: map[string]string{"name": "Alice", "code": "123"},
			want: "Hello Alice, code: 123",
		},
		{
			name: "empty body",
			body: "",
			vars: nil,
			want: "",
		},
		{
			name: "no placeholders",
			body: "Static text",
			vars: map[string]string{"x": "y"},
			want: "Static text",
		},
		{
			name:    "invalid template syntax",
			body:    "Hello {{.name",
			vars:    map[string]string{"name": "Alice"},
			wantErr: true,
		},
		{
			name: "missing variable renders as no value",
			body: "Hello {{.name}}",
			vars: map[string]string{},
			want: "Hello <no value>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := renderTemplate(tt.body, tt.vars)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
