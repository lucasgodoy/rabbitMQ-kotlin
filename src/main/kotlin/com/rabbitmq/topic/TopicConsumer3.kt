import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.topic.TopicProducer.Companion.EXCHANGE_NAME
import com.rabbitmq.topic.TopicProducer.Companion.TOPIC_2

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()
    val queue = channel.queueDeclare("topic_consumer_3", true, false, false, null)
    channel.queueBind(queue.queue, EXCHANGE_NAME, TOPIC_2)

    println(" [*] Waiting for messages.")

    val consumer = object : DefaultConsumer(channel) {
        override fun handleDelivery(
            consumerTag: String,
            envelope: Envelope,
            properties: AMQP.BasicProperties,
            body: ByteArray
        ) {
            val message = String(body, charset("UTF-8"))
            println(" [x] Received from " + envelope.routingKey + ": " + message + " del_tag: " + envelope.deliveryTag)
            channel.basicAck(envelope.deliveryTag, false)
        }
    }
    channel.basicConsume(queue.queue, false, consumer)
}