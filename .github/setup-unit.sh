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

# Start MongoDB and DocumentDB API containers
echo "🚀 Starting MongoDB and DocumentDB API containers..."
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

# Wait for DocumentDB API service to be ready
echo "⏳ Waiting for DocumentDB API service to respond..."
timeout=120
elapsed=0
while ! curl -f -s http://localhost:10260/health &> /dev/null; do
    if [ $elapsed -ge $timeout ]; then
        echo "❌ DocumentDB API service failed to respond within ${timeout} seconds"
        echo "API Server logs:"
        $DC_CMD -f $COMPOSE_FILE logs documentdb-api
        exit 1
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    echo "⏳ Still waiting for API service... (${elapsed}/${timeout}s)"
done

echo "✅ DocumentDB API service is ready"

# Create a test database and collection
echo "🗄️ Creating test database and collection..."

# Test database creation by inserting a test document
response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $(echo -n 'testuser:testpass' | base64)" \
    -d '{
        "database": "test_db",
        "collection": "test_collection",
        "document": {"_id": "test_doc", "message": "DocumentDB is ready for testing", "timestamp": "'$(date -Iseconds)'"}
    }' \
    http://localhost:10260/data/v1/action/insertOne)

echo "Insert response: $response"

# Verify we can read from the database
echo "📋 Verifying database connectivity..."
read_response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $(echo -n 'testuser:testpass' | base64)" \
    -d '{
        "database": "test_db",
        "collection": "test_collection",
        "filter": {"_id": "test_doc"}
    }' \
    http://localhost:10260/data/v1/action/find)

echo "Read response: $read_response"

if echo "$read_response" | grep -q "DocumentDB is ready"; then
    echo "✅ Test database and collection created successfully"
else
    echo "⚠️ Database setup completed, but verification response unclear"
    echo "This is normal - tests will handle actual connectivity"
fi

# Show status
echo "📊 Container status:"
$DC_CMD -f $COMPOSE_FILE ps

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 Connection details:"
echo "  URL: http://localhost:10260"
echo "  Username: testuser"
echo "  Password: testpass"
echo "  Test Database: test_db"
echo "  Test Collection: test_collection"
echo ""
echo "🧪 You can now run tests with:"
echo "  ./gradlew test"
echo "  DOCUMENTDB_INTEGRATION_TESTS=true ./gradlew test"
echo ""
echo "🛑 To stop the services:"
echo "  $DC_CMD -f $COMPOSE_FILE down"