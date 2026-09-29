#!/bin/bash

# ============================================================
# DEPLOY AND TEST RABBITMQ DAG
# ============================================================
# This script will:
# 1. Copy the RabbitMQ test DAG to Airflow
# 2. Install pika on scheduler & workers
# 3. Wait for scheduler to detect it
# 4. Trigger the test
# 5. Show you the logs

set -e

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║           DEPLOYING RABBITMQ TEST DAG TO AIRFLOW                          ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""

# ============================================================
# STEP 1: Copy DAG to Airflow
# ============================================================
echo "STEP 1: Copying RabbitMQ test DAG to Airflow..."
echo "─────────────────────────────────────────────────────────────────────────────"

AIRFLOW_DAGS_DIR="/opt/airflow/dags"
DAG_FILE="rabbitmq_test_dag.py"

# Check if running in Kubernetes pod
if kubectl config current-context &>/dev/null; then
    # Running with kubectl access
    echo "Detected Kubernetes environment"
    
    # Copy via kubectl
    kubectl cp "$DAG_FILE" airflow/airflow-scheduler:"$AIRFLOW_DAGS_DIR/" 2>/dev/null || {
        echo "Note: kubectl cp may need pod name. Trying manual approach..."
        kubectl exec -n airflow airflow-scheduler -- mkdir -p "$AIRFLOW_DAGS_DIR"
        kubectl exec -n airflow airflow-scheduler -- cat << 'EOFDAG' > "$AIRFLOW_DAGS_DIR/$DAG_FILE"
# You'll need to paste the DAG file content here
EOFDAG
    }
    KUBECTL_PREFIX="kubectl exec -n airflow airflow-scheduler --"
else
    # Direct file system access
    if [ ! -d "$AIRFLOW_DAGS_DIR" ]; then
        echo "Error: $AIRFLOW_DAGS_DIR not found"
        echo "Are you running this on the Airflow host?"
        exit 1
    fi
    KUBECTL_PREFIX=""
fi

echo "✓ DAG directory: $AIRFLOW_DAGS_DIR"
echo ""

# ============================================================
# STEP 2: Install pika dependency
# ============================================================
echo "STEP 2: Installing pika library..."
echo "─────────────────────────────────────────────────────────────────────────────"

if [ -n "$KUBECTL_PREFIX" ]; then
    echo "Installing on Airflow scheduler..."
    $KUBECTL_PREFIX pip install pika --quiet
    echo "✓ Installed pika on scheduler"
    
    echo "Installing on Airflow worker(s)..."
    # Get all worker pods
    WORKERS=$(kubectl get pods -n airflow -l component=worker -o jsonpath='{.items[*].metadata.name}')
    for worker in $WORKERS; do
        kubectl exec -n airflow "$worker" -- pip install pika --quiet
        echo "✓ Installed pika on $worker"
    done
else
    pip install pika --quiet
    echo "✓ Installed pika"
fi

echo ""

# ============================================================
# STEP 3: Verify RabbitMQ connection
# ============================================================
echo "STEP 3: Verifying RabbitMQ connection..."
echo "─────────────────────────────────────────────────────────────────────────────"

TEST_CONNECTION=$(cat << 'EOFPYTHON'
import pika
try:
    credentials = pika.PlainCredentials('rmq_user', 'RabbitMQStrongPass123')
    parameters = pika.ConnectionParameters(
        'rabbitmq.data-platform.svc.cluster.local', 5672, 
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2
    )
    conn = pika.BlockingConnection(parameters)
    conn.close()
    print("✓ RabbitMQ is reachable and credentials work")
except Exception as e:
    print(f"✗ RabbitMQ connection failed: {e}")
    import sys
    sys.exit(1)
EOFPYTHON
)

if [ -n "$KUBECTL_PREFIX" ]; then
    $KUBECTL_PREFIX python3 << EOFPYTHON
$TEST_CONNECTION
EOFPYTHON
else
    python3 << EOFPYTHON
$TEST_CONNECTION
EOFPYTHON
fi

echo ""

# ============================================================
# STEP 4: Check if DAG was detected
# ============================================================
echo "STEP 4: Waiting for Airflow scheduler to detect the DAG..."
echo "─────────────────────────────────────────────────────────────────────────────"
echo "Waiting 45 seconds (scheduler scans every ~30s)..."

sleep 45

if [ -n "$KUBECTL_PREFIX" ]; then
    DAG_LIST=$($KUBECTL_PREFIX airflow dags list 2>/dev/null | grep rabbitmq_connection_test || echo "")
else
    DAG_LIST=$(airflow dags list | grep rabbitmq_connection_test || echo "")
fi

if [ -z "$DAG_LIST" ]; then
    echo "✗ DAG not detected yet. Checking scheduler logs..."
    if [ -n "$KUBECTL_PREFIX" ]; then
        kubectl logs -n airflow airflow-scheduler --tail=50 | grep -i rabbitmq || \
            kubectl logs -n airflow airflow-scheduler --tail=50 | grep -i error || \
            echo "No obvious errors in logs. Check manually:"
    fi
    echo ""
    echo "Run these commands to debug:"
    echo "  kubectl logs -n airflow airflow-scheduler -f --tail=100"
    echo "  kubectl logs -n airflow airflow-worker-0 -f --tail=100"
    exit 1
else
    echo "✓ DAG detected: rabbitmq_connection_test"
fi

echo ""

# ============================================================
# STEP 5: Trigger the test
# ============================================================
echo "STEP 5: Triggering the RabbitMQ test DAG..."
echo "─────────────────────────────────────────────────────────────────────────────"

if [ -n "$KUBECTL_PREFIX" ]; then
    RUN_ID=$($KUBECTL_PREFIX airflow dags trigger rabbitmq_connection_test 2>&1 | grep -oP "(?<=Created the following dag runs:\n)\w+" || echo "")
    if [ -z "$RUN_ID" ]; then
        # Try alternate parsing
        RUN_ID=$($KUBECTL_PREFIX airflow dags trigger rabbitmq_connection_test 2>&1 | tail -1)
    fi
    echo "✓ DAG triggered"
    echo ""
    
    # ============================================================
    # STEP 6: Watch the logs
    # ============================================================
    echo "STEP 6: Watching task logs (Ctrl+C to stop)..."
    echo "─────────────────────────────────────────────────────────────────────────────"
    echo ""
    echo "Waiting for tasks to start (5 seconds)..."
    sleep 5
    
    echo ""
    echo "Following worker logs (you should see messages about RabbitMQ):"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    kubectl logs -n airflow airflow-worker-0 -f --tail=100 &
    TAIL_PID=$!
    
    # Wait a bit for output, then show the DAG run status
    sleep 10
    
    echo ""
    echo "DAG run status:"
    $KUBECTL_PREFIX airflow dags list-runs -d rabbitmq_connection_test --limit 5
    
    echo ""
    echo "Stopping log tail (PID $TAIL_PID)..."
    kill $TAIL_PID 2>/dev/null || true
    
    wait $TAIL_PID 2>/dev/null || true
    
else
    # Direct execution
    airflow dags trigger rabbitmq_connection_test
    echo "✓ DAG triggered"
    echo ""
    echo "Watch the logs in your Airflow UI or run:"
    echo "  tail -f ~/.airflow/logs/..."
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║                          DEPLOYMENT COMPLETE!                             ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "1. Open Airflow UI: http://airflow.your-domain.com"
echo "2. Search for 'rabbitmq_connection_test' in the DAG list"
echo "3. Click 'Trigger DAG' to run a new test"
echo "4. Check logs for '✓✓✓ RABBITMQ CONNECTION TEST PASSED ✓✓✓'"
echo ""
echo "If something goes wrong, check:"
echo "  DAG_NOT_VISIBLE_TROUBLESHOOTING.md"
echo ""
