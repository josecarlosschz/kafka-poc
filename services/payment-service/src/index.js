import express from 'express';
import { Kafka, Partitioners } from 'kafkajs';
import { randomUUID } from 'crypto';

const app = express();
const PORT = process.env.SERVICE_PORT || 8001;
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
const ORDERS_TOPIC = process.env.KAFKA_ORDERS_TOPIC || 'orders.v1';
const PAYMENTS_TOPIC = process.env.KAFKA_PAYMENTS_TOPIC || 'payments.v1';

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [KAFKA_BOOTSTRAP],
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const consumer = kafka.consumer({ groupId: 'payment-service-group' });
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner
});

let isReady = false;

async function start() {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: ORDERS_TOPIC, fromBeginning: false });

    console.log(`[PaymentService] Connected to Kafka, listening to topic '${ORDERS_TOPIC}'`);
    isReady = true;

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value ? message.value.toString() : '';
        try {
          const envelope = JSON.parse(rawValue);
          const order = envelope.order;
          const orderId = order.order_id;
          const customerId = order.customer_id;
          const totalAmount = order.total_amount;

          console.log(`[PaymentService] 💳 Received ORDER_CREATED: orderId=${orderId}, customerId=${customerId}, total=$${totalAmount} [partition=${partition}, offset=${message.offset}]`);

          // Simulate payment processing latency
          await new Promise(res => setTimeout(res, 200));

          const paymentId = 'PAY-' + randomUUID().replace(/-/g, '').substring(0, 10).toUpperCase();
          const paymentEvent = {
            payment_id: paymentId,
            order_id: orderId,
            customer_id: customerId,
            amount: totalAmount,
            status: 'SUCCESS',
            processed_at: new Date().toISOString()
          };

          const recordMetadata = await producer.send({
            topic: PAYMENTS_TOPIC,
            messages: [
              {
                key: orderId,
                value: JSON.stringify(paymentEvent)
              }
            ]
          });

          const meta = recordMetadata[0];
          console.log(`[PaymentService] ✅ Payment processed: ${paymentId} for order ${orderId} -> published to ${PAYMENTS_TOPIC} [partition=${meta.partition}, offset=${meta.baseOffset}]`);
        } catch (err) {
          console.error(`[PaymentService] ❌ Error handling message from partition ${partition}:`, err.message);
        }
      }
    });
  } catch (err) {
    console.error('[PaymentService] Initialization error:', err.message);
    setTimeout(start, 5000);
  }
}

app.get('/health', (req, res) => {
  if (isReady) {
    res.status(200).json({ status: 'UP', service: 'payment-service' });
  } else {
    res.status(503).json({ status: 'STARTING', service: 'payment-service' });
  }
});

app.listen(PORT, async () => {
  console.log(`[PaymentService] HTTP server running on port ${PORT}`);
  await start();
});
