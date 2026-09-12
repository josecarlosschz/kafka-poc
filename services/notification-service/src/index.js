import express from 'express';
import { Kafka } from 'kafkajs';

const app = express();
const PORT = process.env.SERVICE_PORT || 8002;
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
const ORDERS_TOPIC = process.env.KAFKA_ORDERS_TOPIC || 'orders.v1';

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [KAFKA_BOOTSTRAP],
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const consumer = kafka.consumer({ groupId: 'notification-service-group' });

let isReady = false;

async function start() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: ORDERS_TOPIC, fromBeginning: false });

    console.log(`[NotificationService] Connected to Kafka, listening to topic '${ORDERS_TOPIC}'`);
    isReady = true;

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value ? message.value.toString() : '';
        try {
          const envelope = JSON.parse(rawValue);
          const order = envelope.order;
          const itemsCount = (order.items && order.items.length) || 0;

          console.log(`[NotificationService] 🔔 ORDER NOTIFICATION: Order ${order.order_id} placed by customer ${order.customer_id}! Total: $${order.total_amount} (${itemsCount} items) [partition=${partition}, offset=${message.offset}]`);
        } catch (err) {
          console.error(`[NotificationService] ❌ Error handling message from partition ${partition}:`, err.message);
        }
      }
    });
  } catch (err) {
    console.error('[NotificationService] Initialization error:', err.message);
    setTimeout(start, 5000);
  }
}

app.get('/health', (req, res) => {
  if (isReady) {
    res.status(200).json({ status: 'UP', service: 'notification-service' });
  } else {
    res.status(503).json({ status: 'STARTING', service: 'notification-service' });
  }
});

app.listen(PORT, async () => {
  console.log(`[NotificationService] HTTP server running on port ${PORT}`);
  await start();
});
