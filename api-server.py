#!/usr/bin/env python3
"""
Simple HTTP API server that simulates DocumentDB REST API endpoints
This bridges HTTP requests to MongoDB operations for testing
"""

import os
import json
import base64
from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# MongoDB connection
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://testuser:testpass@mongodb:27017/test_db?authSource=admin')
client = MongoClient(MONGODB_URI)

def authenticate_request():
    """Simple authentication check"""
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Basic '):
        return False

    try:
        credentials = base64.b64decode(auth_header[6:]).decode('utf-8')
        username, password = credentials.split(':', 1)
        return username == 'testuser' and password == 'testpass'
    except:
        return False

def get_collection(database_name, collection_name):
    """Get MongoDB collection"""
    db = client[database_name]
    return db[collection_name]

@app.route('/data/v1/action/insertOne', methods=['POST'])
def insert_one():
    """Insert a single document"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        document = data['document']

        collection = get_collection(database, collection_name)
        result = collection.insert_one(document)

        return jsonify({
            'insertedId': str(result.inserted_id),
            'insertedCount': 1
        })

    except Exception as e:
        logger.error(f"Insert error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/v1/action/insertMany', methods=['POST'])
def insert_many():
    """Insert multiple documents"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        documents = data['documents']

        collection = get_collection(database, collection_name)
        result = collection.insert_many(documents)

        return jsonify({
            'insertedIds': [str(id) for id in result.inserted_ids],
            'insertedCount': len(result.inserted_ids)
        })

    except Exception as e:
        logger.error(f"Insert many error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/v1/action/find', methods=['POST'])
def find():
    """Find documents"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        filter_doc = data.get('filter', {})
        limit = data.get('limit')
        skip = data.get('skip', 0)

        collection = get_collection(database, collection_name)
        cursor = collection.find(filter_doc).skip(skip)

        if limit:
            cursor = cursor.limit(limit)

        documents = []
        for doc in cursor:
            # Convert ObjectId to string for JSON serialization
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            documents.append(doc)

        return jsonify({
            'documents': documents
        })

    except Exception as e:
        logger.error(f"Find error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/v1/action/aggregate', methods=['POST'])
def aggregate():
    """Execute aggregation pipeline"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        pipeline = data['pipeline']

        collection = get_collection(database, collection_name)
        cursor = collection.aggregate(pipeline)

        documents = []
        for doc in cursor:
            # Convert ObjectId to string for JSON serialization
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            documents.append(doc)

        return jsonify({
            'documents': documents
        })

    except Exception as e:
        logger.error(f"Aggregate error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/v1/action/updateOne', methods=['POST'])
def update_one():
    """Update a single document"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        filter_criteria = data.get('filter', {})
        update_doc = data['update']

        collection = get_collection(database, collection_name)
        result = collection.update_one(filter_criteria, update_doc)

        return jsonify({
            'matchedCount': result.matched_count,
            'modifiedCount': result.modified_count,
            'upsertedId': str(result.upserted_id) if result.upserted_id else None
        })

    except Exception as e:
        logger.error(f"Update one error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/v1/action/updateMany', methods=['POST'])
def update_many():
    """Update multiple documents"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        filter_criteria = data.get('filter', {})
        update_doc = data['update']

        collection = get_collection(database, collection_name)
        result = collection.update_many(filter_criteria, update_doc)

        return jsonify({
            'matchedCount': result.matched_count,
            'modifiedCount': result.modified_count
        })

    except Exception as e:
        logger.error(f"Update many error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/v1/action/deleteOne', methods=['POST'])
def delete_one():
    """Delete a single document"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        filter_criteria = data.get('filter', {})

        collection = get_collection(database, collection_name)
        result = collection.delete_one(filter_criteria)

        return jsonify({
            'deletedCount': result.deleted_count
        })

    except Exception as e:
        logger.error(f"Delete one error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/v1/action/deleteMany', methods=['POST'])
def delete_many():
    """Delete multiple documents"""
    if not authenticate_request():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        data = request.get_json()
        database = data['database']
        collection_name = data['collection']
        filter_criteria = data.get('filter', {})

        collection = get_collection(database, collection_name)
        result = collection.delete_many(filter_criteria)

        return jsonify({
            'deletedCount': result.deleted_count
        })

    except Exception as e:
        logger.error(f"Delete many error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        client.admin.command('ping')
        return jsonify({'status': 'healthy', 'message': 'DocumentDB API server is running'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting DocumentDB API server...")
    logger.info(f"MongoDB URI: {MONGODB_URI}")
    app.run(host='0.0.0.0', port=10260, debug=True)