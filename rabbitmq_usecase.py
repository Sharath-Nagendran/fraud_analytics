#!/usr/bin/env python3
"""
Test RabbitMQ Connection (Works with Generic Type)
Can be run as standalone Python script or in Airflow DAG
"""

import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_rabbitmq_direct():
    """
    Test RabbitMQ directly (doesn't require Airflow connection)
    Use this for quick validation
    """
    logger.info("Testing RabbitMQ direct connection...")
    
    try:
        import pika
        
        host = "rabbitmq.data-platform.svc.cluster.local"
        port = 5672
        username = "rmq_user"
        password = "RabbitMQStrongPass123"
        
        # Step 1: Create credentials
        logger.info(f"✓ Connecting to {host}:{port}")
        credentials = pika.PlainCredentials(username, password)
        
        # Step 2: Create connection parameters
        parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        logger.info("✓ Connection parameters created")
        
        # Step 3: Create connection
        connection = pika.BlockingConnection(parameters)
        logger.info("✓ Connected to RabbitMQ broker")
        
        # Step 4: Create channel
        channel = connection.channel()
        logger.info("✓ Channel created")
        
        # Step 5: Declare queue
        queue_name = "airflow_test_queue"
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"✓ Queue declared: {queue_name}")
        
        # Step 6: Publish message
        test_message = json.dumps({
            "test_id": "airflow_connection_test",
            "timestamp": datetime.now().isoformat(),
            "status": "success",
            "message": "Connection test successful"
        })
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=test_message.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
        logger.info(f"✓ Message published to queue: {queue_name}")
        logger.info(f"  Message content: {test_message}")
        
        # Step 7: Verify by consuming
        received_messages = []
        
        def callback(ch, method, properties, body):
            message = json.loads(body.decode('utf-8'))
            received_messages.append(message)
            logger.info(f"✓ Message consumed: {message['test_id']}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        
        # Consume with timeout
        logger.info("Waiting for message...")
        channel.connection.call_later(2, lambda: channel.stop_consuming())
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        
        # Step 8: Close connection
        connection.close()
        logger.info("✓ Connection closed")
        
        logger.info("\n" + "="*60)
        logger.info("✓ RABBITMQ CONNECTION TEST: PASSED")
        logger.info("="*60)
        logger.info(f"✓ Published: 1 message")
        logger.info(f"✓ Consumed: {len(received_messages)} message(s)")
        logger.info(f"✓ Queue name: {queue_name}")
        
        return {
            "status": "PASS",
            "message": "RabbitMQ connection successful",
            "operations": ["connect", "channel_create", "queue_declare", "publish", "consume"],
            "host": host,
            "port": port
        }
        
    except Exception as e:
        logger.error(f"✗ Error: {str(e)}", exc_info=True)
        logger.info("\n" + "="*60)
        logger.info("✗ RABBITMQ CONNECTION TEST: FAILED")
        logger.info("="*60)
        
        return {
            "status": "FAIL",
            "message": str(e),
            "error": type(e).__name__
        }


def test_rabbitmq_via_airflow_connection():
    """
    Test RabbitMQ via Airflow connection
    Requires: airflow connection to exist
    """
    logger.info("Testing RabbitMQ via Airflow connection...")
    
    try:
        from airflow.models import Connection
        from airflow.exceptions import AirflowException
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        # Get connection from Airflow metadata
        from airflow import settings
        Session = sessionmaker(bind=settings.engine)
        session = Session()
        
        conn = session.query(Connection).filter_by(conn_id='rabbitmq_default').first()
        
        if not conn:
            raise AirflowException("Connection 'rabbitmq_default' not found in Airflow")
        
        logger.info(f"✓ Found connection: {conn.conn_id}")
        logger.info(f"  Type: {conn.conn_type}")
        logger.info(f"  Host: {conn.host}")
        logger.info(f"  Port: {conn.port}")
        logger.info(f"  Login: {conn.login}")
        
        # Extract connection details
        host = conn.host
        port = conn.port or 5672
        username = conn.login
        password = conn.password
        
        # Now test with pika
        import pika
        
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2
        )
        
        connection = pika.BlockingConnection(parameters)
        logger.info("✓ Connected via Airflow connection")
        
        channel = connection.channel()
        logger.info("✓ Channel created")
        
        # Test queue operations
        queue_name = "airflow"
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"✓ Queue verified: {queue_name}")
        
        connection.close()
        logger.info("✓ Connection closed")
        
        return {
            "status": "PASS",
            "message": "Airflow RabbitMQ connection successful",
            "conn_id": "rabbitmq_default"
        }
        
    except Exception as e:
        logger.error(f"✗ Error: {str(e)}", exc_info=True)
        return {
            "status": "FAIL",
            "message": str(e),
            "error": type(e).__name__
        }


def main():
    """Run both tests"""
    logger.info("\n" + "="*60)
    logger.info("RABBITMQ CONNECTION TESTING")
    logger.info("="*60 + "\n")
    
    # Test 1: Direct connection (always works)
    logger.info("[1/2] Testing direct connection...")
    result1 = test_rabbitmq_direct()
    
    # Test 2: Via Airflow (requires connection to exist)
    logger.info("\n[2/2] Testing via Airflow connection...")
    result2 = test_rabbitmq_via_airflow_connection()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    logger.info(f"Direct test: {result1['status']}")
    logger.info(f"Airflow test: {result2['status']}")
    
    return result1, result2


if __name__ == "__main__":
    main()
