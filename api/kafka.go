package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

type TaskResult struct {
	JobID    string `json:"job_id"`
	ExitCode int    `json:"exit_code"`
	TimedOut bool   `json:"timed_out"`
	Stdout   string `json:"stdout"`
}

type waitMap struct {
	mu      sync.Mutex
	waiters map[string]chan TaskResult
}

func newWaitMap() *waitMap {
	return &waitMap{
		waiters: make(map[string]chan TaskResult),
	}
}

func (w *waitMap) register(jobID string) chan TaskResult {
	ch := make(chan TaskResult, 1)
	w.mu.Lock()
	w.waiters[jobID] = ch
	w.mu.Unlock()
	return ch
}

func (w *waitMap) deliver(jobID string, result TaskResult) {
	w.mu.Lock()
	ch, ok := w.waiters[jobID]
	if ok {
		delete(w.waiters, jobID)
	}
	w.mu.Unlock()

	if ok {
		ch <- result
	}
}

func (w *waitMap) unregister(jobID string) {
	w.mu.Lock()
	delete(w.waiters, jobID)
	w.mu.Unlock()
}

type kafkaClient struct {
	writer  *kafka.Writer
	reader  *kafka.Reader
	waitMap *waitMap
}

func newKafkaClient(broker string) *kafkaClient {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "invocations",
	})

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "results",
		GroupID: "api-results",
	})

	return &kafkaClient{
		writer:  writer,
		reader:  reader,
		waitMap: newWaitMap(),
	}
}

func (k *kafkaClient) close() {
	k.writer.Close()
	k.reader.Close()
}

func (k *kafkaClient) publish(ctx context.Context, jobID string, payload []byte) error {
	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(jobID),
		Value: payload,
	})
}

func (k *kafkaClient) readResults(ctx context.Context) {
	for {
		msg, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Println("results read error:", err)
			continue
		}

		var result TaskResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			log.Printf("parse result error: %v", err)
			k.reader.CommitMessages(ctx, msg)
			continue
		}

		k.waitMap.deliver(result.JobID, result)

		if err := k.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}
