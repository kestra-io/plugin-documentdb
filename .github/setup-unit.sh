#!/bin/bash

# Setup script for Kestra DocumentDB Plugin Unit Tests
# This script sets up a local DocumentDB instance for testing both locally and in CI

set -e

echo "🐳 Setting up DocumentDB for unit tests..."

# Check if Docker and Docker Compose are available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

# Check for Docker Compose (both v1 and v2)
if ! command -v docker-compose &> /dev/null && ! command -v docker compose &> /dev/null; then
    echo "❌ Docker Compose is not installed or not in PATH"
    exit 1
fi

# Use docker compose (v2) if available, otherwise fall back to docker-compose (v1)
if command -v docker compose &> /dev/null; then
    DC_CMD="docker compose"
    COMPOSE_FILE="docker-compose-ci.yml"
else
    DC_CMD="docker-compose"
    COMPOSE_FILE="docker-compose-ci.yml"
fi

# Stop and remove any existing containers
echo "🧹 Cleaning up existing containers..."
$DC_CMD -f $COMPOSE_FILE down -v --remove-orphans || true

# Start MongoDB container
echo "🚀 Starting MongoDB container..."
$DC_CMD -f $COMPOSE_FILE up -d --build

# Wait for containers to start
echo "⏳ Waiting for containers to start..."
timeout=120
elapsed=0
while ! $DC_CMD -f $COMPOSE_FILE ps | grep -q "mongodb.*Up"; do
    if [ $elapsed -ge $timeout ]; then
        echo "❌ MongoDB container failed to start within ${timeout} seconds"
        $DC_CMD -f $COMPOSE_FILE logs mongodb
        exit 1
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    echo "⏳ Still waiting for MongoDB container... (${elapsed}/${timeout}s)"
done

echo "✅ MongoDB container is running"

# Show status
echo "📊 Container status:"
$DC_CMD -f $COMPOSE_FILE ps

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 MongoDB connection details:"
echo "  Host: localhost"
echo "  Port: 27017"
echo "  Username: testuser"
echo "  Password: testpass"
echo "  Test Database: test_db"
echo ""
echo "🧪 You can now run tests with:"
echo "  ./gradlew test"
echo "  DOCUMENTDB_INTEGRATION_TESTS=true ./gradlew test"
echo ""
echo "🛑 To stop the services:"
echo "  $DC_CMD -f $COMPOSE_FILE down"