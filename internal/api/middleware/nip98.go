package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// nip98AuthedHandler is invoked after successful NIP-98 validation, with the
// authenticated author pubkey passed in.
type nip98AuthedHandler func(w http.ResponseWriter, r *http.Request, authorPubkey string)

const nip98Kind = 27235
const nip98TimeWindow = 60 // seconds

// NIP98Auth wraps a handler with NIP-98 HTTP Auth validation.
// On success, the authenticated author pubkey (hex) is passed to next.
// On failure, responds 401 with a JSON error and does not invoke next.
func NIP98Auth(next nip98AuthedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Read and replay body so downstream handler can decode it.
		// (The payload tag is validated against the exact bytes we read here.)
		var body []byte
		if r.Body != nil {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				writeJSONError(w, http.StatusBadRequest, "failed to read body")
				return
			}
			body = b
			r.Body = io.NopCloser(bytes.NewReader(body))
		}

		ev, err := parseNIP98Header(r.Header.Get("Authorization"))
		if err != nil {
			writeJSONError(w, http.StatusUnauthorized, err.Error())
			return
		}

		if err := validateNIP98Event(ev, r, body); err != nil {
			writeJSONError(w, http.StatusUnauthorized, err.Error())
			return
		}

		ok, err := ev.CheckSignature()
		if err != nil || !ok {
			writeJSONError(w, http.StatusUnauthorized, "invalid signature")
			return
		}

		next(w, r, ev.PubKey)
	}
}

func parseNIP98Header(h string) (*nostr.Event, error) {
	if h == "" {
		return nil, errNIP98("missing Authorization header")
	}
	const prefix = "Nostr "
	if !strings.HasPrefix(h, prefix) {
		return nil, errNIP98("Authorization must start with 'Nostr '")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(h, prefix))
	if err != nil {
		// Also accept URL-safe / unpadded base64 variants for resilience.
		if raw2, err2 := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(h, prefix)); err2 == nil {
			raw = raw2
		} else {
			return nil, errNIP98("bad base64 in Authorization")
		}
	}
	var ev nostr.Event
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, errNIP98("bad event JSON")
	}
	return &ev, nil
}

func validateNIP98Event(ev *nostr.Event, r *http.Request, body []byte) error {
	if ev.Kind != nip98Kind {
		return errNIP98("wrong kind")
	}
	now := time.Now().Unix()
	created := int64(ev.CreatedAt)
	delta := now - created
	if delta < 0 {
		delta = -delta
	}
	if delta > nip98TimeWindow {
		return errNIP98("timestamp out of window")
	}

	uTag := tagValue(ev.Tags, "u")
	methodTag := tagValue(ev.Tags, "method")
	if uTag == "" || methodTag == "" {
		return errNIP98("missing u or method tag")
	}

	if !strings.EqualFold(methodTag, r.Method) {
		return errNIP98("method tag mismatch")
	}
	if !urlMatches(uTag, r) {
		return errNIP98("u tag mismatch")
	}

	// Payload tag is required for POST/PUT/PATCH with a non-empty body,
	// optional otherwise (per NIP-98).
	if len(body) > 0 && requiresPayloadTag(r.Method) {
		payloadTag := tagValue(ev.Tags, "payload")
		if payloadTag == "" {
			return errNIP98("missing payload tag")
		}
		sum := sha256.Sum256(body)
		if !strings.EqualFold(payloadTag, hex.EncodeToString(sum[:])) {
			return errNIP98("payload tag mismatch")
		}
	}
	return nil
}

func requiresPayloadTag(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return true
	}
	return false
}

// urlMatches compares the NIP-98 u tag against the actual request URL.
// Supports reverse-proxy deployments via X-Forwarded-Proto / X-Forwarded-Host.
func urlMatches(uTag string, r *http.Request) bool {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	host := r.Host
	if xfh := r.Header.Get("X-Forwarded-Host"); xfh != "" {
		host = xfh
	}
	reconstructed := scheme + "://" + host + r.URL.RequestURI()
	return uTag == reconstructed
}

func tagValue(tags nostr.Tags, name string) string {
	for _, t := range tags {
		if len(t) >= 2 && t[0] == name {
			return t[1]
		}
	}
	return ""
}

// errNIP98 wraps a sentinel error to keep the reasons user-visible yet terse.
type nip98Err string

func (e nip98Err) Error() string { return string(e) }
func errNIP98(msg string) error  { return nip98Err(msg) }

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":   http.StatusText(status),
		"message": message,
	})
}
