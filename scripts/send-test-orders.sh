#!/usr/bin/env bash

# ==============================================================================
# Kafka Event-Driven Architecture PoC: Test Order Generator (Node.js + KafkaJS)
# ==============================================================================
# Sends simulated customer orders to the Node.js Order Service API (POST /api/v1/orders).
# Demonstrates key-based partitioning (by order_id) and decoupled consumption.
# ==============================================================================

set -euo pipefail

BASE_URL="${ORDER_SERVICE_URL:-http://localhost:8000}"
ENDPOINT="${BASE_URL}/api/v1/orders"

echo "======================================================================"
echo "🚀 Sending Simulated Orders to Kafka PoC (Node.js LTS v22 + KafkaJS)"
echo "📍 Target endpoint: ${ENDPOINT}"
echo "======================================================================"

# Check health first
printf "Checking Order Service health... "
if curl -s -f "${BASE_URL}/health" > /dev/null; then
    echo "✅ UP"
else
    echo "❌ Down or unreachable at ${BASE_URL}/health"
    echo "Make sure containers are up: docker compose up -d"
    exit 1
fi

echo ""
echo "Submitting 6 test orders across different customers..."
echo "----------------------------------------------------------------------"

CUSTOMERS=("CUST-1001" "CUST-2045" "CUST-3099" "CUST-4100" "CUST-5555" "CUST-9999")
PRODUCTS=(
    "PROD-101:Mechanical Keyboard:1:129.99"
    "PROD-202:Ultra-wide Monitor 34-inch:1:499.50"
    "PROD-303:USB-C Ergonomic Mouse:2:35.00"
    "PROD-404:Noise-Canceling Headphones:1:199.99"
    "PROD-505:Thunderbolt 4 Dock:1:249.00"
    "PROD-606:4K Webcam with Microphone:1:89.90"
)

for i in "${!CUSTOMERS[@]}"; do
    CUST="${CUSTOMERS[$i]}"
    PROD_SPEC="${PRODUCTS[$i]}"
    IFS=':' read -r P_ID P_NAME P_QTY P_PRICE <<< "$PROD_SPEC"

    PAYLOAD=$(cat <<EOF
{
  "customer_id": "${CUST}",
  "items": [
    {
      "product_id": "${P_ID}",
      "name": "${P_NAME}",
      "quantity": ${P_QTY},
      "unit_price": ${P_PRICE}
    }
  ]
}
EOF
)

    printf "\n📦 [Order #%d] Customer: %s | Product: %s\n" "$((i+1))" "${CUST}" "${P_NAME}"
    RESPONSE=$(curl -s -X POST "${ENDPOINT}" \
        -H "Content-Type: application/json" \
        -d "${PAYLOAD}")

    echo "Response: ${RESPONSE}"
    sleep 0.5
done

echo ""
echo "======================================================================"
echo "✅ All 6 orders sent successfully!"
echo "👉 Open Kafka UI at: http://localhost:8080"
echo "👉 Topic 'orders.v1': Verify 3 partitions received keyed events."
echo "👉 Topic 'payments.v1': Verify payment confirmations produced."
echo "👉 Inspect logs with:"
echo "   docker compose logs -f payment-service notification-service"
echo "======================================================================"
