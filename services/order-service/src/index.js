import express from 'express';
import { Kafka, Partitioners } from 'kafkajs';
import { randomUUID } from 'crypto';

const app = express();
app.use(express.json());

const PORT = process.env.SERVICE_PORT || 8000;
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
const TOPIC = process.env.KAFKA_ORDERS_TOPIC || 'orders.v1';

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: [KAFKA_BOOTSTRAP],
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner
});

let isReady = false;

async function initKafka() {
  try {
    await producer.connect();
    isReady = true;
    console.log(`[OrderService] Connected to Kafka broker at ${KAFKA_BOOTSTRAP}`);
  } catch (err) {
    console.error('[OrderService] Failed to connect to Kafka:', err.message);
    setTimeout(initKafka, 5000);
  }
}

app.get('/health', (req, res) => {
  if (isReady) {
    res.status(200).json({ status: 'UP', service: 'order-service' });
  } else {
    res.status(503).json({ status: 'STARTING', service: 'order-service' });
  }
});

app.post('/api/v1/orders', async (req, res) => {
  try {
    const { customer_id, items } = req.body;

    if (!customer_id || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({
        error: 'Invalid order payload. customer_id and items are required.'
      });
    }

    const calculatedTotal = items.reduce((acc, item) => {
      return acc + (Number(item.quantity) * Number(item.unit_price));
    }, 0);

    const orderId = 'ORD-' + randomUUID().replace(/-/g, '').substring(0, 10).toUpperCase();
    const eventId = 'evt-' + randomUUID().replace(/-/g, '');
    const createdAt = new Date().toISOString();

    const order = {
      order_id: orderId,
      customer_id,
      items: items.map(item => ({
        product_id: item.product_id,
        name: item.name,
        quantity: Number(item.quantity),
        unit_price: Number(item.unit_price)
      })),
      total_amount: Math.round(calculatedTotal * 100) / 100,
      status: 'PENDING',
      created_at: createdAt
    };

    const envelope = {
      event_id: eventId,
      event_type: 'ORDER_CREATED',
      timestamp: createdAt,
      order
    };

    const recordMetadata = await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(envelope)
        }
      ]
    });

    const meta = recordMetadata[0];
    console.log(`[OrderService] Published order ${orderId} to topic ${TOPIC} [partition ${meta.partition}, offset ${meta.baseOffset}]`);

    return res.status(200).json({
      success: true,
      message: 'Order accepted and event published to Kafka',
      order,
      event_id: eventId,
      topic: TOPIC,
      partition: meta.partition,
      offset: parseInt(meta.baseOffset, 10)
    });
  } catch (error) {
    console.error('[OrderService] Error processing order:', error);
    return res.status(500).json({
      error: 'Failed to publish order event',
      details: error.message
    });
  }
});

app.listen(PORT, async () => {
  console.log(`[OrderService] HTTP server running on port ${PORT}`);
  await initKafka();
});
