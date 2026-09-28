package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type JobStatus string

const (
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
)

type Job struct {
	ID        string    `json:"job_id"`
	Status    JobStatus `json:"status"`
	Input     string    `json:"input,omitempty"`
	Result    any       `json:"result,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// in memory job store

type JobStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func NewJobStore() *JobStore {
	return &JobStore{jobs: make(map[string]*Job)}
}

func (s *JobStore) Create(input string) *Job {
	job := &Job{
		ID:        newID(),
		Status:    StatusProcessing,
		Input:     input,
		CreatedAt: time.Now(),
	}
	s.mu.Lock()
	s.jobs[job.ID] = job
	s.mu.Unlock()
	return job
}

func (s *JobStore) Get(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *job, true
}

func (s *JobStore) Complete(id string, result any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[id]; ok {
		job.Status = StatusCompleted
		job.Result = result
	}
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

//  Background worker

// (1 minute delay)
func processJob(store *JobStore, id, input string) {
	log.Printf("[worker] job %s started", id)
	time.Sleep(1 * time.Minute) // simulated heavy work

	result := map[string]any{
		"message":      "processed: " + input,
		"processed_at": time.Now().Format(time.RFC3339),
	}
	store.Complete(id, result)
	log.Printf("[worker] job %s completed", id)
}

type Server struct {
	store *JobStore
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// POST /jobs starts a job, returns job_id + status "processing"
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {

	var body struct {
		Input string `json:"input"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	job := s.store.Create(body.Input)
	go processJob(s.store, job.ID, body.Input) //run in backgroudn

	writeJSON(w, http.StatusAccepted, map[string]string{
		"job_id": job.ID,
		"status": string(job.Status),
	})
}

// GET /jobs/{id} status or result if done
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {

	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job id required"})
		return
	}

	job, ok := s.store.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	if job.Status == StatusCompleted {
		writeJSON(w, http.StatusOK, map[string]any{
			"job_id": job.ID,
			"status": job.Status,
			"result": job.Result,
		})
		return
	}

	// if still processing return status only
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": job.ID,
		"status": job.Status,
	})
}

func main() {
	srv := &Server{store: NewJobStore()}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", srv.createJob)
	mux.HandleFunc("GET /jobs/", srv.getJob)

	addr := ":8080"
	log.Printf("server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
