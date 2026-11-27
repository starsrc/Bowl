package sandboxcli

import (
	"bytes"
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/starsrc/bowl/client/internal/sandbox"
)

const maxFileBody = 512 << 20

type apiServer struct {
	createMu sync.Mutex
}

type apiCreateRequest struct {
	Template       string `json:"template"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	CPUs           int    `json:"cpus"`
	MemoryMiB      int    `json:"memory_mib"`
}

type apiSandbox struct {
	SandboxID string `json:"sandbox_id"`
	Status    string `json:"status"`
	Template  string `json:"template"`
	Address   string `json:"address,omitempty"`
	Created   string `json:"created"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type apiCommandRequest struct {
	Cmd            string            `json:"cmd"`
	Argv           []string          `json:"argv"`
	Cwd            string            `json:"cwd"`
	Envs           map[string]string `json:"envs"`
	Stdin          string            `json:"stdin"`
	Background     bool              `json:"background"`
	TimeoutSeconds int               `json:"timeout_seconds"`
}

type apiCommandResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	RunID    string `json:"run_id,omitempty"`
}

func newServeCmd() *cobra.Command {
	var listen, apiKey string
	var srv apiServer
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve the sandbox HTTP API used by the SDKs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			apiKey = strings.TrimSpace(cmp.Or(apiKey, os.Getenv("BOWL_SANDBOX_API_KEY")))
			host, _, err := net.SplitHostPort(listen)
			if err != nil {
				return usageExitErr(fmt.Errorf("invalid --listen: %w", err), false)
			}
			if ip := net.ParseIP(host); apiKey == "" && (ip == nil || !ip.IsLoopback()) && host != "localhost" {
				return usageExitErr(errors.New("listening beyond loopback needs --api-key or $BOWL_SANDBOX_API_KEY"), false)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "bowl-sandbox API listening on http://%s\n", listen)
			server := &http.Server{Addr: listen, Handler: srv.routes(apiKey), ReadHeaderTimeout: 10 * time.Second}
			return runtimeExitErr(server.ListenAndServe())
		},
	}
	cmd.Flags().StringVar(&listen, "listen", "127.0.0.1:7070", "Address to listen on")
	cmd.Flags().StringVar(&apiKey, "api-key", "", "Require this bearer token (default $BOWL_SANDBOX_API_KEY); mandatory beyond loopback")
	return cmd
}

func (s *apiServer) routes(apiKey string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sandboxes", s.create)
	mux.HandleFunc("GET /sandboxes", s.list)
	mux.HandleFunc("GET /sandboxes/{id}", s.withSandbox(s.get))
	mux.HandleFunc("DELETE /sandboxes/{id}", s.withSandbox(s.kill))
	mux.HandleFunc("POST /sandboxes/{id}/commands", s.withSandbox(s.run))
	mux.HandleFunc("GET /sandboxes/{id}/commands/{run}", s.withSandbox(s.wait))
	mux.HandleFunc("GET /sandboxes/{id}/files", s.withSandbox(s.readFile))
	mux.HandleFunc("PUT /sandboxes/{id}/files", s.withSandbox(s.writeFile))
	mux.HandleFunc("POST /sandboxes/{id}/timeout", s.withSandbox(s.setTimeout))
	if apiKey == "" {
		return mux
	}
	want := []byte("Bearer " + apiKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			writeAPIError(w, http.StatusUnauthorized, errors.New("missing or wrong API key"))
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *apiServer) withSandbox(h func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !vm.Exists(id) {
			writeAPIError(w, http.StatusNotFound, fmt.Errorf("sandbox %s not found", id))
			return
		}
		h(w, r, id)
	}
}

func (s *apiServer) create(w http.ResponseWriter, r *http.Request) {
	var req apiCreateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	o := createOpts{
		Template: req.Template, CPUs: req.CPUs, MemoryMiB: req.MemoryMiB,
		Timeout: time.Duration(req.TimeoutSeconds) * time.Second,
	}
	s.createMu.Lock()
	id, err := sboxCreate(&o)
	s.createMu.Unlock()
	if err != nil {
		writeCLIError(w, err)
		return
	}
	s.get(w, r, id)
}

func (s *apiServer) get(w http.ResponseWriter, _ *http.Request, id string) {
	info, err := vm.Info(id)
	if err != nil {
		writeAPIError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, toAPISandbox(&info))
}

func toAPISandbox(info *sandbox.Info) apiSandbox {
	out := apiSandbox{SandboxID: info.ID, Status: info.Status, Template: info.Template, Address: info.Address, Created: info.Created}
	if deadline, ok := sboxDeadline(info.ID); ok {
		out.ExpiresAt = deadline.Format(time.RFC3339)
	}
	return out
}

func (s *apiServer) list(w http.ResponseWriter, _ *http.Request) {
	rows, err := vm.List()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]apiSandbox, 0, len(rows))
	for i := range rows {
		out = append(out, toAPISandbox(&rows[i]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created < out[j].Created })
	writeJSON(w, http.StatusOK, out)
}

func (s *apiServer) kill(w http.ResponseWriter, r *http.Request, id string) {
	if err := sboxKill(r.Context(), id); err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *apiServer) setTimeout(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		TimeoutSeconds int `json:"timeout_seconds"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.TimeoutSeconds <= 0 {
		sboxClearDeadline(id)
	} else if err := sboxSetDeadline(id, time.Duration(req.TimeoutSeconds)*time.Second); err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	s.get(w, r, id)
}

func (s *apiServer) run(w http.ResponseWriter, r *http.Request, id string) {
	var req apiCommandRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	argv := req.Argv
	if (req.Cmd == "") == (len(argv) == 0) {
		writeAPIError(w, http.StatusBadRequest, errors.New("set exactly one of cmd and argv"))
		return
	}
	if req.Cmd != "" {
		argv = []string{"sh", "-c", req.Cmd}
	}
	ctx := r.Context()
	cwd := req.Cwd
	if req.Background {
		runID, err := vm.ExecBackground(ctx, id, cwd, argv)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, apiCommandResult{RunID: runID})
		return
	}
	env := make([]string, 0, len(req.Envs))
	for k, v := range req.Envs {
		env = append(env, k+"="+v)
	}
	if req.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	var stdout, stderr bytes.Buffer
	code, err := vm.Exec(ctx, id, cwd, argv, sandbox.ExecOptions{Env: env},
		strings.NewReader(req.Stdin), &stdout, &stderr)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		writeAPIError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, apiCommandResult{ExitCode: code, Stdout: stdout.String(), Stderr: stderr.String()})
}

func (s *apiServer) wait(w http.ResponseWriter, r *http.Request, id string) {
	runID := r.PathValue("run")
	code, err := vm.Wait(r.Context(), id, runID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	var out bytes.Buffer
	if err = vm.Logs(r.Context(), id, runID, false, &out, io.Discard); err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, apiCommandResult{ExitCode: code, Stdout: out.String(), RunID: runID})
}

func (s *apiServer) readFile(w http.ResponseWriter, r *http.Request, id string) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeAPIError(w, http.StatusBadRequest, errors.New("path is required"))
		return
	}
	var out, errOut bytes.Buffer
	code, err := vm.Exec(r.Context(), id, "/", []string{"cat", "--", path}, sandbox.ExecOptions{}, nil, &out, &errOut)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	if code != 0 {
		writeAPIError(w, http.StatusNotFound, errors.New(strings.TrimSpace(errOut.String())))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(out.Bytes())
}

func (s *apiServer) writeFile(w http.ResponseWriter, r *http.Request, id string) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeAPIError(w, http.StatusBadRequest, errors.New("path is required"))
		return
	}
	var errOut bytes.Buffer
	script := `mkdir -p "$(dirname -- "$1")" && cat > "$1"`
	code, err := vm.Exec(r.Context(), id, "/", []string{"sh", "-c", script, "sh", path}, sandbox.ExecOptions{},
		http.MaxBytesReader(w, r.Body, maxFileBody), io.Discard, &errOut)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	if code != 0 {
		writeAPIError(w, http.StatusBadRequest, errors.New(strings.TrimSpace(errOut.String())))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		writeAPIError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeCLIError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if exitErr, ok := errors.AsType[*exitError](err); ok && exitErr.code == 2 {
		status = http.StatusBadRequest
	}
	writeAPIError(w, status, err)
}
