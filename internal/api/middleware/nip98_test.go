package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// signAuthEvent builds and signs a NIP-98 auth event for the given request
// parameters. Test helper.
func signAuthEvent(t *testing.T, sk, method, url string, body []byte, createdAt int64) string {
	t.Helper()
	tags := nostr.Tags{
		nostr.Tag{"u", url},
		nostr.Tag{"method", method},
	}
	if len(body) > 0 {
		sum := sha256.Sum256(body)
		tags = append(tags, nostr.Tag{"payload", hex.EncodeToString(sum[:])})
	}
	ev := nostr.Event{
		Kind:      nip98Kind,
		CreatedAt: nostr.Timestamp(createdAt),
		Tags:      tags,
		Content:   "",
	}
	if err := ev.Sign(sk); err != nil {
		t.Fatalf("sign failed: %v", err)
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return "Nostr " + base64.StdEncoding.EncodeToString(raw)
}

func mustGenKey(t *testing.T) (sk, pk string) {
	t.Helper()
	sk = nostr.GeneratePrivateKey()
	pk, err := nostr.GetPublicKey(sk)
	if err != nil {
		t.Fatalf("derive pubkey: %v", err)
	}
	return
}

// runRequest fires a request against a handler wrapped with NIP98Auth, using
// httptest.NewServer so r.Host/Scheme are realistic for the u-tag reconstruct.
func runRequest(t *testing.T, path string, method string, body []byte, authHeader string) (int, string, string) {
	t.Helper()
	var gotPubkey string
	h := NIP98Auth(func(w http.ResponseWriter, r *http.Request, pubkey string) {
		gotPubkey = pubkey
		// Echo back body to verify it's been replayed correctly.
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	})

	srv := httptest.NewServer(http.HandlerFunc(h))
	defer srv.Close()

	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respBody), gotPubkey
}

// buildURL returns the URL that NIP98Auth will reconstruct for matching.
func buildURL(serverURL, path string) string {
	return serverURL + path
}

func TestNIP98_HappyPath(t *testing.T) {
	sk, pk := mustGenKey(t)
	body := []byte(`{"target":"xyz"}`)

	// First set up server so we know its URL for the u tag.
	var authHeader string
	var gotPubkey string
	mux := http.NewServeMux()
	mux.Handle("/vouch", NIP98Auth(func(w http.ResponseWriter, r *http.Request, pubkey string) {
		gotPubkey = pubkey
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	authHeader = signAuthEvent(t, sk, "POST", srv.URL+"/vouch", body, time.Now().Unix())

	req, _ := http.NewRequest("POST", srv.URL+"/vouch", bytes.NewReader(body))
	req.Header.Set("Authorization", authHeader)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, respBody)
	}
	if gotPubkey != pk {
		t.Fatalf("expected pubkey %q, got %q", pk, gotPubkey)
	}
	if string(respBody) != string(body) {
		t.Fatalf("body was not replayed to handler; got %q", respBody)
	}
}

func TestNIP98_MissingHeader(t *testing.T) {
	status, _, _ := runRequest(t, "/vouch", "POST", []byte(`{}`), "")
	if status != 401 {
		t.Fatalf("expected 401, got %d", status)
	}
}

func TestNIP98_WrongScheme(t *testing.T) {
	status, _, _ := runRequest(t, "/vouch", "POST", []byte(`{}`), "Basic xyz")
	if status != 401 {
		t.Fatalf("expected 401, got %d", status)
	}
}

func TestNIP98_BadBase64(t *testing.T) {
	status, _, _ := runRequest(t, "/vouch", "POST", []byte(`{}`), "Nostr !!!not-base64!!!")
	if status != 401 {
		t.Fatalf("expected 401, got %d", status)
	}
}

func TestNIP98_WrongKind(t *testing.T) {
	sk, _ := mustGenKey(t)
	// Build server to get URL.
	srv := httptest.NewServer(NIP98Auth(func(w http.ResponseWriter, r *http.Request, pk string) {}))
	defer srv.Close()

	ev := nostr.Event{
		Kind:      1, // wrong kind
		CreatedAt: nostr.Timestamp(time.Now().Unix()),
		Tags:      nostr.Tags{{"u", srv.URL + "/vouch"}, {"method", "POST"}},
	}
	if err := ev.Sign(sk); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(ev)
	auth := "Nostr " + base64.StdEncoding.EncodeToString(raw)

	req, _ := http.NewRequest("POST", srv.URL+"/vouch", bytes.NewReader(nil))
	req.Header.Set("Authorization", auth)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestNIP98_TimestampTooOld(t *testing.T) {
	sk, _ := mustGenKey(t)
	srv := httptest.NewServer(NIP98Auth(func(w http.ResponseWriter, r *http.Request, pk string) {}))
	defer srv.Close()

	auth := signAuthEvent(t, sk, "POST", srv.URL+"/vouch", nil, time.Now().Unix()-120)

	req, _ := http.NewRequest("POST", srv.URL+"/vouch", nil)
	req.Header.Set("Authorization", auth)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401 for old timestamp, got %d", resp.StatusCode)
	}
}

func TestNIP98_MethodMismatch(t *testing.T) {
	sk, _ := mustGenKey(t)
	srv := httptest.NewServer(NIP98Auth(func(w http.ResponseWriter, r *http.Request, pk string) {}))
	defer srv.Close()

	// Sign with GET but send POST.
	auth := signAuthEvent(t, sk, "GET", srv.URL+"/vouch", nil, time.Now().Unix())
	req, _ := http.NewRequest("POST", srv.URL+"/vouch", nil)
	req.Header.Set("Authorization", auth)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401 for method mismatch, got %d", resp.StatusCode)
	}
}

func TestNIP98_URLMismatch(t *testing.T) {
	sk, _ := mustGenKey(t)
	srv := httptest.NewServer(NIP98Auth(func(w http.ResponseWriter, r *http.Request, pk string) {}))
	defer srv.Close()

	auth := signAuthEvent(t, sk, "POST", "https://other.example/vouch", nil, time.Now().Unix())
	req, _ := http.NewRequest("POST", srv.URL+"/vouch", nil)
	req.Header.Set("Authorization", auth)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401 for URL mismatch, got %d", resp.StatusCode)
	}
}

func TestNIP98_PayloadMismatch(t *testing.T) {
	sk, _ := mustGenKey(t)
	srv := httptest.NewServer(NIP98Auth(func(w http.ResponseWriter, r *http.Request, pk string) {}))
	defer srv.Close()

	signedBody := []byte(`{"target":"a"}`)
	actualBody := []byte(`{"target":"b"}`)

	auth := signAuthEvent(t, sk, "POST", srv.URL+"/vouch", signedBody, time.Now().Unix())
	req, _ := http.NewRequest("POST", srv.URL+"/vouch", bytes.NewReader(actualBody))
	req.Header.Set("Authorization", auth)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401 for payload mismatch, got %d", resp.StatusCode)
	}
}

func TestNIP98_InvalidSignature(t *testing.T) {
	sk, _ := mustGenKey(t)
	srv := httptest.NewServer(NIP98Auth(func(w http.ResponseWriter, r *http.Request, pk string) {}))
	defer srv.Close()

	auth := signAuthEvent(t, sk, "POST", srv.URL+"/vouch", nil, time.Now().Unix())

	// Corrupt one byte of the base64-encoded signature at the tail end.
	b := []byte(auth)
	// Flip a character a few positions from the end (before padding).
	idx := len(b) - 10
	if b[idx] == 'A' {
		b[idx] = 'B'
	} else {
		b[idx] = 'A'
	}
	corrupted := string(b)

	req, _ := http.NewRequest("POST", srv.URL+"/vouch", nil)
	req.Header.Set("Authorization", corrupted)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// Could be 401 (sig invalid) or 401 (base64 invalid) — both fine.
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401 for corrupted auth, got %d", resp.StatusCode)
	}
}

func TestNIP98_URLReconstructWithForwardedProto(t *testing.T) {
	sk, pk := mustGenKey(t)
	var gotPubkey string
	h := NIP98Auth(func(w http.ResponseWriter, r *http.Request, pubkey string) {
		gotPubkey = pubkey
	})
	srv := httptest.NewServer(http.HandlerFunc(h))
	defer srv.Close()

	// The u tag claims https, but actual srv is http. X-Forwarded-Proto=https
	// should make the reconstruction succeed.
	host := strings.TrimPrefix(srv.URL, "http://")
	auth := signAuthEvent(t, sk, "POST", "https://"+host+"/vouch", nil, time.Now().Unix())

	req, _ := http.NewRequest("POST", srv.URL+"/vouch", nil)
	req.Header.Set("Authorization", auth)
	req.Header.Set("X-Forwarded-Proto", "https")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 with X-Forwarded-Proto, got %d", resp.StatusCode)
	}
	if gotPubkey != pk {
		t.Fatalf("expected pk %q, got %q", pk, gotPubkey)
	}
}
