#!/bin/bash

# Setup script for Kestra DocumentDB Plugin Unit Tests
# This script starts the MongoDB test service locally and in CI

set -e

echo "🐳 Setting up MongoDB for DocumentDB plugin tests..."

# Check if Docker and Docker Compose are available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

# Check for Docker Compose (both v1 and v2)
if docker compose version &> /dev/null; then
    DC_CMD="docker compose"
elif command -v docker-compose &> /dev/null; then
    DC_CMD="docker-compose"
else
    echo "❌ Docker Compose is not installed or not in PATH"
    exit 1
fi

# Use docker compose (v2) if available, otherwise fall back to docker-compose (v1)
COMPOSE_FILE="docker-compose-ci.yml"

# Start MongoDB container
echo "🚀 Starting MongoDB container..."
$DC_CMD -f "$COMPOSE_FILE" up -d --build

# Wait for containers to start
echo "⏳ Waiting for containers to start..."
timeout=120
elapsed=0
while ! $DC_CMD -f "$COMPOSE_FILE" exec -T mongodb mongosh --quiet \
    --username testuser \
    --password testpass \
    --authenticationDatabase admin \
    --eval 'db.adminCommand("ping").ok' 2>/dev/null | grep -q '^1$'; do
    if [ $elapsed -ge $timeout ]; then
        echo "❌ MongoDB container failed to start within ${timeout} seconds"
        $DC_CMD -f "$COMPOSE_FILE" logs mongodb
        exit 1
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    echo "⏳ Still waiting for MongoDB container... (${elapsed}/${timeout}s)"
done

echo "✅ MongoDB container is running"

# Show status
echo "📊 Container status:"
$DC_CMD -f "$COMPOSE_FILE" ps

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 MongoDB test connection details:"
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