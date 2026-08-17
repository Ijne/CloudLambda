package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"lambda/internal/sandbox"
	"lambda/internal/vars"

	"github.com/segmentio/kafka-go"
)

type Task struct {
	JobID       string       `json:"job_id"`
	EnvType     vars.EnvType `json:"env_type"`
	CodeSource  string       `json:"code_source"`
	MemoryLimit int64        `json:"memory_limit"`
	CPUQuota    int64        `json:"cpu_quota"`
	TimeoutSec  int          `json:"timeout_sec"`
}

type TaskResult struct {
	JobID    string `json:"job_id"`
	ExitCode int    `json:"exit_code"`
	TimedOut bool   `json:"timed_out"`
	Stdout   string `json:"stdout"`
}

func parseTask(data []byte) (*Task, error) {
	var t Task
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func marshalResult(jobID string, res *sandbox.Result) ([]byte, error) {
	tr := TaskResult{
		JobID:    jobID,
		ExitCode: res.ExitCode,
		TimedOut: res.TimedOut,
		Stdout:   string(res.Stdout),
	}
	return json.Marshal(tr)
}

func poolSize() int {
	val := os.Getenv("WORKER_POOL_SIZE")
	if val == "" {
		return 10
	}
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		log.Printf("invalid WORKER_POOL_SIZE=%q, using default 10", val)
		return 10
	}
	return n
}

func handle(ctx context.Context, msg kafka.Message, writer *kafka.Writer) {
	task, err := parseTask(msg.Value)
	if err != nil {
		log.Printf("parse task error: %v", err)
		return
	}

	log.Printf("executing job %s (runtime: %s)", task.JobID, task.EnvType)

	res := sandbox.Run(sandbox.Config{
		EnvType:     task.EnvType,
		CodeSource:  task.CodeSource,
		MemoryLimit: task.MemoryLimit,
		CPUQuota:    task.CPUQuota,
		TimeLimit:   time.Duration(task.TimeoutSec) * time.Second,
	})
	if res == nil {
		log.Printf("job %s: execution returned nil", task.JobID)
		return
	}

	log.Printf("job %s done: exit_code=%d timed_out=%v", task.JobID, res.ExitCode, res.TimedOut)

	data, err := marshalResult(task.JobID, res)
	if err != nil {
		log.Printf("job %s: marshal result error: %v", task.JobID, err)
		return
	}

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(task.JobID),
		Value: data,
	}); err != nil {
		log.Printf("job %s: write result error: %v", task.JobID, err)
	}
}

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "results",
	})
	defer writer.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "executor-group",
		Topic:   "invocations",
	})
	defer reader.Close()

	sem := make(chan struct{}, poolSize())

	log.Printf("worker started, pool size: %d", cap(sem))

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("read error:", err)
			continue
		}

		sem <- struct{}{}
		go func(msg kafka.Message) {
			defer func() { <-sem }()
			handle(context.Background(), msg, writer)
		}(msg)
	}
}
