package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Configuration
var (
	port                string
	monolithURL         string
	moviesServiceURL    string
	eventsServiceURL    string
	gradualMigration    bool
	moviesMigrationPercent int
)

func main() {
	// Load configuration from environment variables
	port = getEnv("PORT", "8000")
	monolithURL = getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	eventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")
	
	gradualMigrationStr := getEnv("GRADUAL_MIGRATION", "false")
	gradualMigration = gradualMigrationStr == "true"
	
	moviesMigrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "0")
	var err error
	moviesMigrationPercent, err = strconv.Atoi(moviesMigrationPercentStr)
	if err != nil {
		log.Printf("Warning: Invalid MOVIES_MIGRATION_PERCENT value '%s', defaulting to 0", moviesMigrationPercentStr)
		moviesMigrationPercent = 0
	}

	// Validate URLs
	if _, err := url.Parse(monolithURL); err != nil {
		log.Fatalf("Invalid MONOLITH_URL: %v", err)
	}
	if _, err := url.Parse(moviesServiceURL); err != nil {
		log.Fatalf("Invalid MOVIES_SERVICE_URL: %v", err)
	}
	if _, err := url.Parse(eventsServiceURL); err != nil {
		log.Fatalf("Invalid EVENTS_SERVICE_URL: %v", err)
	}

	// Initialize random seed for percentage-based routing
	rand.Seed(time.Now().UnixNano())

	// Setup routes
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/", apiHandler)

	log.Printf("Starting Strangler Fig Proxy on port %s", port)
	log.Printf("Configuration:")
	log.Printf("  Monolith URL: %s", monolithURL)
	log.Printf("  Movies Service URL: %s", moviesServiceURL)
	log.Printf("  Events Service URL: %s", eventsServiceURL)
	log.Printf("  Gradual Migration: %v", gradualMigration)
	log.Printf("  Movies Migration Percent: %d%%", moviesMigrationPercent)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func apiHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Route events to events service
	if strings.HasPrefix(path, "/api/events") {
		proxyRequest(w, r, eventsServiceURL)
		return
	}

	// Route movies based on Strangler Fig pattern
	if strings.HasPrefix(path, "/api/movies") {
		routeMoviesRequest(w, r)
		return
	}

	// Route all other requests to monolith
	proxyRequest(w, r, monolithURL)
}

func routeMoviesRequest(w http.ResponseWriter, r *http.Request) {
	targetURL := monolithURL

	if !gradualMigration {
		// If gradual migration is disabled, route all requests to movies service
		targetURL = moviesServiceURL
		log.Printf("Routing /api/movies to Movies Service (gradual migration disabled)")
	} else {
		// Percentage-based routing
		randomPercent := rand.Intn(100)
		if randomPercent < moviesMigrationPercent {
			targetURL = moviesServiceURL
			log.Printf("Routing /api/movies to Movies Service (%d%% migration, random: %d)", moviesMigrationPercent, randomPercent)
		} else {
			targetURL = monolithURL
			log.Printf("Routing /api/movies to Monolith (%d%% migration, random: %d)", moviesMigrationPercent, randomPercent)
		}
	}

	proxyRequest(w, r, targetURL)
}

func proxyRequest(w http.ResponseWriter, r *http.Request, targetBaseURL string) {
	// Parse target URL
	target, err := url.Parse(targetBaseURL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid target URL: %v", err), http.StatusInternalServerError)
		return
	}

	// Create reverse proxy
	proxy := httputil.NewSingleHostReverseProxy(target)

	// Modify the request
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.URL.Path = r.URL.Path
		req.URL.RawQuery = r.URL.RawQuery
		req.Host = target.Host
	}

	// Handle errors
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error: %v", err)
		errorResponse := map[string]string{
			"error": fmt.Sprintf("Proxy error: %v", err),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(errorResponse)
	}

	// Copy request body if present
	if r.Body != nil {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	// Serve the request
	proxy.ServeHTTP(w, r)
}
