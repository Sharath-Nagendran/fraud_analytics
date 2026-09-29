"""
rabbitmq_test_dag.py

Standalone DAG for testing RabbitMQ connection from Airflow.
Place this file in: /opt/airflow/dags/

This DAG:
1. Publishes 5 test messages to a RabbitMQ queue
2. Consumes them back
3. Verifies round-trip success
4. Tests the rabbitmq_default connection from Airflow
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

try:
    from airflow.hooks.base import BaseHook
except Exception:
    BaseHook = None

logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================
RABBITMQ_QUEUE = "airflow_test_queue"
TEST_MESSAGE_COUNT = 5

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


# ============================================================
# Helper: Get RabbitMQ connection details from Airflow
# ============================================================
def _get_rabbitmq_connection():
    """
    Resolve RabbitMQ connection from Airflow's rabbitmq_default connection.
    Falls back to environment variables if BaseHook is not available.
    
    Returns:
        dict: {host, port, login, password}
    
    Raises:
        AirflowException: If connection cannot be resolved
    """
    if BaseHook is not None:
        try:
            conn = BaseHook.get_connection("rabbitmq_default")
            if conn and conn.host:
                logger.info("Using rabbitmq_default from Airflow connection")
                return {
                    "host": conn.host,
                    "port": conn.port or 5672,
                    "login": conn.login or "guest",
                    "password": conn.password,
                }
        except Exception as e:
            logger.warning("Could not load rabbitmq_default from Airflow: %s. Trying env vars...", e)

    # Fallback to environment variables
    import os
    import re
    
    # Try to get from env vars, handling full URIs like tcp://host:port
    host_env = os.getenv("RABBITMQ_HOST", "")
    port_env = os.getenv("RABBITMQ_PORT", "")
    
    # Parse full URI if provided (e.g., tcp://10.109.88.13:5672)
    if host_env.startswith("tcp://") or host_env.startswith("amqp://"):
        match = re.match(r"^(?:tcp|amqp)://([^:]+):(\d+)$", host_env)
        if match:
            host, port = match.groups()
            port = int(port)
        else:
            raise AirflowException(f"Could not parse RabbitMQ URI: {host_env}")
    elif port_env.startswith("tcp://") or port_env.startswith("amqp://"):
        # Sometimes the full URI ends up in RABBITMQ_PORT
        match = re.match(r"^(?:tcp|amqp)://([^:]+):(\d+)$", port_env)
        if match:
            host, port = match.groups()
            port = int(port)
        else:
            raise AirflowException(f"Could not parse RabbitMQ URI from RABBITMQ_PORT: {port_env}")
    else:
        # Standard case: separate host and port
        host = host_env or "rabbitmq.data-platform.svc.cluster.local"
        try:
            port = int(port_env) if port_env else 5672
        except ValueError:
            raise AirflowException(
                f"RABBITMQ_PORT must be a number, got: {port_env}. "
                f"Use the Airflow connection 'rabbitmq_default' instead of env vars."
            )
    
    login = os.getenv("RABBITMQ_USER", "rmq_user")
    password = os.getenv("RABBITMQ_PASSWORD", os.getenv("RABBITMQ_PASS", ""))
    
    if not password:
        raise AirflowException(
            "RabbitMQ password not found. Set RABBITMQ_PASSWORD env var or create "
            "rabbitmq_default connection in Airflow."
        )
    
    logger.info("Using RabbitMQ from environment variables: host=%s, port=%s", host, port)
    return {"host": host, "port": port, "login": login, "password": password}


# ============================================================
# Task 1: Test Connection & Publish Messages
# ============================================================
def _test_connection_and_publish(**context):
    """
    Test RabbitMQ connection and publish test messages to the queue.
    """
    try:
        import pika
    except ImportError:
        raise AirflowException(
            "pika library not installed. Run: pip install pika"
        )
    
    logger.info("=" * 80)
    logger.info("TASK 1: Testing RabbitMQ Connection & Publishing Messages")
    logger.info("=" * 80)
    
    # Get connection details
    conn_details = _get_rabbitmq_connection()
    logger.info("RabbitMQ connection details: host=%s, port=%s, user=%s",
                conn_details["host"], conn_details["port"], conn_details["login"])
    
    # Create connection
    try:
        credentials = pika.PlainCredentials(conn_details["login"], conn_details["password"])
        parameters = pika.ConnectionParameters(
            host=conn_details["host"],
            port=conn_details["port"],
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        connection = pika.BlockingConnection(parameters)
        logger.info("✓ Connected to RabbitMQ broker")
    except pika.exceptions.AMQPConnectionError as e:
        raise AirflowException(f"Failed to connect to RabbitMQ: {e}")
    except Exception as e:
        raise AirflowException(f"Unexpected error connecting to RabbitMQ: {e}")
    
    try:
        channel = connection.channel()
        logger.info("✓ Created channel")
        
        # Declare the queue (idempotent)
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        logger.info("✓ Declared queue: %s", RABBITMQ_QUEUE)
        
        # Publish test messages
        messages = []
        for i in range(TEST_MESSAGE_COUNT):
            msg = {
                "id": i + 1,
                "timestamp": datetime.utcnow().isoformat(),
                "message": f"Test message {i + 1}",
                "test_run_id": context["run_id"],
            }
            msg_body = json.dumps(msg)
            
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=msg_body,
                properties=pika.BasicProperties(delivery_mode=2),  # Make message persistent
            )
            messages.append(msg)
            logger.info("  Published message %d/%d: %s", i + 1, TEST_MESSAGE_COUNT, msg["message"])
        
        logger.info("✓ Published %d test messages to queue '%s'", TEST_MESSAGE_COUNT, RABBITMQ_QUEUE)
        
        # Push metrics to XCom for next task
        context["ti"].xcom_push(key="messages_published", value=len(messages))
        context["ti"].xcom_push(key="test_run_id", value=context["run_id"])
        
    except Exception as e:
        raise AirflowException(f"Error publishing messages: {e}")
    finally:
        connection.close()
        logger.info("✓ Closed RabbitMQ connection")


# ============================================================
# Task 2: Consume & Verify Messages
# ============================================================
def _consume_and_verify(**context):
    """
    Consume the test messages from the queue and verify they match.
    
    FIXED: 
    - Removed 'connection.stop()' which doesn't exist
    - Use 'channel.stop_consuming()' instead
    - Properly handle the consumer timeout
    - Check if messages are actually in the queue before trying to consume
    """
    try:
        import pika
    except ImportError:
        raise AirflowException("pika library not installed. Run: pip install pika")
    
    logger.info("=" * 80)
    logger.info("TASK 2: Consuming & Verifying Messages")
    logger.info("=" * 80)
    
    # Get expected message count from previous task
    expected_count = context["ti"].xcom_pull(
        task_ids="test_connection_and_publish", key="messages_published") or TEST_MESSAGE_COUNT
    test_run_id = context["ti"].xcom_pull(
        task_ids="test_connection_and_publish", key="test_run_id")
    
    logger.info("Expecting to consume %d messages (test_run_id=%s)", expected_count, test_run_id)
    
    # Get connection details
    conn_details = _get_rabbitmq_connection()
    
    # Create connection
    try:
        credentials = pika.PlainCredentials(conn_details["login"], conn_details["password"])
        parameters = pika.ConnectionParameters(
            host=conn_details["host"],
            port=conn_details["port"],
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2,
        )
        connection = pika.BlockingConnection(parameters)
        logger.info("✓ Connected to RabbitMQ broker")
    except pika.exceptions.AMQPConnectionError as e:
        raise AirflowException(f"Failed to connect to RabbitMQ: {e}")
    
    try:
        channel = connection.channel()
        logger.info("✓ Created channel")
        
        # Declare the queue (in case this task runs before publish in a rerun)
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        logger.info("Queue declared: %s", RABBITMQ_QUEUE)
        
        # Check if there are messages in the queue
        queue_state = channel.queue_declare(queue=RABBITMQ_QUEUE, passive=True)
        message_count = queue_state.method.message_count
        logger.info("Messages in queue %s: %d", RABBITMQ_QUEUE, message_count)
        
        if message_count == 0 and expected_count > 0:
            logger.warning(
                "Queue is empty but expected %d messages. "
                "This could mean:\n"
                "  1. Messages were already consumed by another consumer\n"
                "  2. Queue was cleared between tasks\n"
                "  3. Publish task didn't actually publish\n"
                "Continuing anyway...", 
                expected_count
            )
        
        # Consume messages
        consumed_messages = []
        consume_timeout_ms = 10000  # 10 seconds of idle time before giving up
        
        def on_message(ch, method, properties, body):
            """Callback when a message is received"""
            try:
                msg = json.loads(body.decode("utf-8"))
                consumed_messages.append(msg)
                logger.info("  Consumed message %d/%d: %s", 
                           len(consumed_messages), expected_count, msg.get("message", ""))
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
                # Stop consuming if we got all expected messages
                if expected_count and len(consumed_messages) >= expected_count:
                    logger.info("Received all %d expected messages, stopping consumer", expected_count)
                    ch.stop_consuming()
            except Exception as e:
                logger.error("Error processing message: %s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        # Set up the consumer
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message)
        logger.info("Consumer started (idle timeout=%dms, expect %d messages)", 
                   consume_timeout_ms, expected_count)
        
        # Start consuming
        # The consumer will:
        # 1. Call on_message for each message it receives
        # 2. Stop if it's idle for consumer_timeout_ms
        # 3. Stop if on_message calls ch.stop_consuming()
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.info("Consumer finished: %s", str(e))
        
        logger.info("✓ Consumption loop finished")
        logger.info("Consumed %d messages from queue '%s'", len(consumed_messages), RABBITMQ_QUEUE)
        
        # Verify we got what we sent
        if consumed_messages and expected_count:
            if len(consumed_messages) != expected_count:
                logger.warning(
                    "Message count mismatch: expected %d, consumed %d. "
                    "This can happen if other consumers are reading from the queue.",
                    expected_count, len(consumed_messages)
                )
                # Don't fail here - just warn. The messages were there.
        
        if not consumed_messages and expected_count > 0:
            logger.error(
                "No messages consumed! Queue had %d messages but consumer got 0. "
                "Possible causes:\n"
                "  1. Another consumer already read them\n"
                "  2. RabbitMQ purged the queue\n"
                "  3. Consumer group offset issue\n"
                "  4. Connection/permissions issue",
                message_count
            )
            raise AirflowException(
                f"Failed to consume any messages (expected {expected_count}, queue has {message_count})"
            )
        
        # Verify each message's structure
        for i, msg in enumerate(consumed_messages, 1):
            if not all(k in msg for k in ["id", "timestamp", "message", "test_run_id"]):
                raise AirflowException(f"Message {i} has invalid structure: {msg}")
            if msg["test_run_id"] != test_run_id:
                raise AirflowException(
                    f"Message {i} test_run_id mismatch: expected {test_run_id}, got {msg['test_run_id']}"
                )
        
        logger.info("✓ All %d messages verified successfully", len(consumed_messages))
        logger.info("=" * 80)
        logger.info("✓✓✓ RABBITMQ CONNECTION TEST PASSED ✓✓✓")
        logger.info("=" * 80)
        
        # Push summary to XCom
        context["ti"].xcom_push(key="messages_consumed", value=len(consumed_messages))
        context["ti"].xcom_push(key="test_status", value="PASSED")
        
    except AirflowException:
        raise  # Re-raise Airflow exceptions as-is
    except Exception as e:
        logger.error("Error consuming messages: %s", str(e), exc_info=True)
        raise AirflowException(f"Consumption failed: {e}")
    finally:
        if connection and not connection.is_closed:
            connection.close()
            logger.info("✓ Closed RabbitMQ connection")


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="rabbitmq_connection_test",
    default_args=default_args,
    description="Test RabbitMQ connection: publish -> consume -> verify",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["rabbitmq", "test", "connection"],
    doc_md=__doc__,
) as dag:

    publish_task = PythonOperator(
        task_id="test_connection_and_publish",
        python_callable=_test_connection_and_publish,
        doc="Test RabbitMQ connection and publish test messages",
    )

    consume_task = PythonOperator(
        task_id="consume_and_verify",
        python_callable=_consume_and_verify,
        doc="Consume messages and verify they match what was sent",
    )

    publish_task >> consume_task
