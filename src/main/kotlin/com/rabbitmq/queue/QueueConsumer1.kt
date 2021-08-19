package com.rabbitmq.queue

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.queue.QueueProducer.Companion.EXECUTION_ID_1

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.queueDeclare(EXECUTION_ID_1, true, false, true, null)
    println(" [*] Waiting for messages. To exit press CTRL+C")

    channel.basicQos(1)

    val consumer = object : DefaultConsumer(channel) {
        override fun handleDelivery(
            consumerTag: String,
            envelope: Envelope,
            properties: AMQP.BasicProperties,
            body: ByteArray
        ) {
            val message = String(body, charset("UTF-8"))
            println(" [x] Received '$message' with delivery tag: ${envelope.deliveryTag}")
            println(" [x] Done")
            channel.basicAck(envelope.deliveryTag, false)
        }
    }
    channel.basicConsume(EXECUTION_ID_1, false, consumer)
}