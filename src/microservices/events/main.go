package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Configuration
var (
	port         string
	kafkaBrokers string
)

// Event types
type MovieEvent struct {
	MovieID     int       `json:"movie_id"`
	Title       string    `json:"title"`
	Action      string    `json:"action"`
	UserID      *int      `json:"user_id,omitempty"`
	Rating      *float64  `json:"rating,omitempty"`
	Genres      []string  `json:"genres,omitempty"`
	Description string    `json:"description,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"`
	Email     *string   `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType *string   `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status    string      `json:"status"`
	Partition int         `json:"partition"`
	Offset    int64       `json:"offset"`
	Event     interface{} `json:"event"`
}

// Kafka writers (one per topic)
var (
	movieWriter   *kafka.Writer
	userWriter    *kafka.Writer
	paymentWriter *kafka.Writer
	writersOnce   sync.Once
)

func main() {
	// Load configuration
	port = getEnv("PORT", "8082")
	kafkaBrokers = getEnv("KAFKA_BROKERS", "localhost:9092")

	// Initialize Kafka writers
	initWriters()

	// Start Kafka consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startConsumer(ctx)

	// Setup HTTP routes
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)

	log.Printf("Starting Events Service on port %s", port)
	log.Printf("Kafka Brokers: %s", kafkaBrokers)

	// Handle graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	server := &http.Server{Addr: ":" + port}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	<-sigchan
	log.Println("Shutting down...")
	cancel()
	closeWriters()
	server.Shutdown(context.Background())
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func initWriters() {
	writersOnce.Do(func() {
		brokers := strings.Split(kafkaBrokers, ",")
		for i := range brokers {
			brokers[i] = strings.TrimSpace(brokers[i])
		}

		movieWriter = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    "movie-events",
			Balancer: &kafka.LeastBytes{},
		}

		userWriter = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    "user-events",
			Balancer: &kafka.LeastBytes{},
		}

		paymentWriter = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    "payment-events",
			Balancer: &kafka.LeastBytes{},
		}

		log.Println("Kafka writers initialized")
	})
}

func closeWriters() {
	if movieWriter != nil {
		movieWriter.Close()
	}
	if userWriter != nil {
		userWriter.Close()
	}
	if paymentWriter != nil {
		paymentWriter.Close()
	}
}

func startConsumer(ctx context.Context) {
	brokers := strings.Split(kafkaBrokers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	topics := []string{"movie-events", "user-events", "payment-events"}

	// Create readers for each topic
	for _, topic := range topics {
		go func(topicName string) {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:  brokers,
				Topic:    topicName,
				GroupID:  "events-service-consumer",
				MinBytes: 10e3, // 10KB
				MaxBytes: 10e6, // 10MB
			})
			defer reader.Close()

			log.Printf("Subscribed to topic: %s", topicName)

			for {
				select {
				case <-ctx.Done():
					log.Printf("Stopping consumer for topic: %s", topicName)
					return
				default:
					msg, err := reader.ReadMessage(ctx)
					if err != nil {
						if err == context.Canceled {
							return
						}
						log.Printf("Consumer error for topic %s: %v", topicName, err)
						time.Sleep(1 * time.Second)
						continue
					}

					// Log the consumed event
					log.Printf("Consumed event from topic %s [partition %d, offset %d]: %s",
						msg.Topic,
						msg.Partition,
						msg.Offset,
						string(msg.Value))

					// Process event based on topic
					switch topicName {
					case "movie-events":
						var event MovieEvent
						if err := json.Unmarshal(msg.Value, &event); err == nil {
							log.Printf("Processed Movie Event: MovieID=%d, Title=%s, Action=%s",
								event.MovieID, event.Title, event.Action)
						} else {
							log.Printf("Failed to unmarshal movie event: %v", err)
						}
					case "user-events":
						var event UserEvent
						if err := json.Unmarshal(msg.Value, &event); err == nil {
							log.Printf("Processed User Event: UserID=%d, Action=%s",
								event.UserID, event.Action)
						} else {
							log.Printf("Failed to unmarshal user event: %v", err)
						}
					case "payment-events":
						var event PaymentEvent
						if err := json.Unmarshal(msg.Value, &event); err == nil {
							log.Printf("Processed Payment Event: PaymentID=%d, UserID=%d, Amount=%.2f, Status=%s",
								event.PaymentID, event.UserID, event.Amount, event.Status)
						} else {
							log.Printf("Failed to unmarshal payment event: %v", err)
						}
					}
				}
			}
		}(topic)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Validate required fields
	if event.MovieID == 0 || event.Title == "" || event.Action == "" {
		http.Error(w, "Missing required fields: movie_id, title, action", http.StatusBadRequest)
		return
	}

	// Publish to Kafka
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to serialize event: %v", err), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = movieWriter.WriteMessages(ctx, kafka.Message{
		Value: eventJSON,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to produce message: %v", err), http.StatusInternalServerError)
		return
	}

	// Note: kafka-go doesn't return partition/offset synchronously in WriteMessages
	// We'll use a default response
	response := EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event UserEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Validate required fields
	if event.UserID == 0 || event.Action == "" {
		http.Error(w, "Missing required fields: user_id, action", http.StatusBadRequest)
		return
	}

	// Publish to Kafka
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to serialize event: %v", err), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = userWriter.WriteMessages(ctx, kafka.Message{
		Value: eventJSON,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to produce message: %v", err), http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Validate required fields
	if event.PaymentID == 0 || event.UserID == 0 || event.Amount == 0 || event.Status == "" {
		http.Error(w, "Missing required fields: payment_id, user_id, amount, status", http.StatusBadRequest)
		return
	}

	// Publish to Kafka
	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to serialize event: %v", err), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = paymentWriter.WriteMessages(ctx, kafka.Message{
		Value: eventJSON,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to produce message: %v", err), http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}
