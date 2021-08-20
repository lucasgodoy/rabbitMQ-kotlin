package com.rabbitmq.concurrency

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.concurrency.Producer.Companion.QUEUE_ID

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()
    // Consumer will not create the queue. It assumes in this example that the queue is created by the producer.
    // If we wanted the queue to be created by the producer or the consumer (which starts first), we need to use the
    // complete queueDeclare syntax.
    val queue = channel.queueDeclarePassive(QUEUE_ID)
    println(" [*] Waiting for messages.")

    channel.basicQos(1)

    val consumer = object : DefaultConsumer(channel) {
        override fun handleDelivery(
            consumerTag: String,
            envelope: Envelope,
            properties: AMQP.BasicProperties,
            body: ByteArray
        ) {
            val message = String(body, charset("UTF-8"))
            println(" [x] Consumer 1 received '$message' with delivery tag: ${envelope.deliveryTag}")
            println(" [x] Done")
            channel.basicAck(envelope.deliveryTag, false)
            Thread.sleep(2000)
        }
    }
    channel.basicConsume(queue.queue, false, consumer)
}