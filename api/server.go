package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"lambda/internal/db"
	"lambda/internal/vars"
)

type server struct {
	repo   *db.FunctionRepo
	kafka  *kafkaClient
	fnRoot string
}

func newServer(repo *db.FunctionRepo, kafka *kafkaClient) *server {
	fnRoot := os.Getenv("FUNCTIONS_ROOT")
	if fnRoot == "" {
		fnRoot = "/var/cloudfunctions/functions"
	}
	return &server{
		repo:   repo,
		kafka:  kafka,
		fnRoot: fnRoot,
	}
}

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /functions", s.handleDeploy)
	mux.HandleFunc("GET /functions", s.handleList)
	mux.HandleFunc("GET /functions/{id}", s.handleGet)
	mux.HandleFunc("DELETE /functions/{id}", s.handleDelete)
	mux.HandleFunc("POST /functions/{id}/invoke", s.handleInvoke)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// POST /functions
func (s *server) handleDeploy(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form")
		return
	}

	name := r.FormValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	runtime := vars.EnvType(r.FormValue("runtime"))
	switch runtime {
	case vars.EnvTypeGo, vars.EnvTypePython, vars.EnvTypeJava:
	default:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unsupported runtime: %s", runtime))
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "file is required")
		return
	}
	defer file.Close()

	id := newID()
	dir := filepath.Join(s.fnRoot, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create function directory")
		return
	}

	codePath := filepath.Join(dir, header.Filename)
	dst, err := os.Create(codePath)
	if err != nil {
		os.RemoveAll(dir)
		writeError(w, http.StatusInternalServerError, "failed to save file")
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		os.RemoveAll(dir)
		writeError(w, http.StatusInternalServerError, "failed to write file")
		return
	}

	fn := &db.Function{
		ID:          id,
		Name:        name,
		Runtime:     runtime,
		CodePath:    codePath,
		MemoryLimit: 512 * 1024 * 1024,
		CPUQuota:    50000,
		TimeoutSec:  10,
	}
	fmt.Sscan(r.FormValue("memory_limit"), &fn.MemoryLimit)
	fmt.Sscan(r.FormValue("cpu_quota"), &fn.CPUQuota)
	fmt.Sscan(r.FormValue("timeout_sec"), &fn.TimeoutSec)

	if err := s.repo.Save(r.Context(), fn); err != nil {
		os.RemoveAll(dir)
		writeError(w, http.StatusInternalServerError, "failed to save function")
		return
	}

	writeJSON(w, http.StatusCreated, fn)
}

// GET /functions
func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	fns, err := s.repo.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list functions")
		return
	}
	writeJSON(w, http.StatusOK, fns)
}

// GET /functions/{id}
func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	fn, err := s.repo.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, db.ErrNotFound) {
		writeError(w, http.StatusNotFound, "function not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get function")
		return
	}
	writeJSON(w, http.StatusOK, fn)
}

// DELETE /functions/{id}
func (s *server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fn, err := s.repo.Get(r.Context(), id)
	if errors.Is(err, db.ErrNotFound) {
		writeError(w, http.StatusNotFound, "function not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get function")
		return
	}

	if err := s.repo.Delete(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete function")
		return
	}

	os.RemoveAll(filepath.Join(s.fnRoot, fn.ID))
	w.WriteHeader(http.StatusNoContent)
}

// POST /functions/{id}/invoke
func (s *server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	fn, err := s.repo.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, db.ErrNotFound) {
		writeError(w, http.StatusNotFound, "function not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get function")
		return
	}

	jobID := newID()

	task := map[string]any{
		"job_id":       jobID,
		"env_type":     fn.Runtime,
		"code_source":  fn.CodePath,
		"memory_limit": fn.MemoryLimit,
		"cpu_quota":    fn.CPUQuota,
		"timeout_sec":  fn.TimeoutSec,
	}
	payload, err := json.Marshal(task)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to marshal task")
		return
	}

	// Регистрируем канал до отправки в Kafka — чтобы не пропустить быстрый ответ
	ch := s.kafka.waitMap.register(jobID)
	defer s.kafka.waitMap.unregister(jobID)

	if err := s.kafka.publish(r.Context(), jobID, payload); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to publish task")
		return
	}

	// Ждём результата с таймаутом чуть больше чем timeout функции
	timeout := time.Duration(fn.TimeoutSec+5) * time.Second
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	select {
	case result := <-ch:
		writeJSON(w, http.StatusOK, result)
	case <-ctx.Done():
		writeError(w, http.StatusGatewayTimeout, "function execution timed out")
	}
}
