import com.rabbitmq.client.*
import com.rabbitmq.direct.RoutingProducer.Companion.EXCHANGE_NAME
import com.rabbitmq.direct.RoutingProducer.Companion.EXECUTION_ID_1

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.queueDeclare(EXECUTION_ID_1, true, false, true, null)
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, null)
    channel.queueBind(EXECUTION_ID_1, EXCHANGE_NAME, EXECUTION_ID_1)

    println(" [*] Waiting for messages. To exit press CTRL+C")

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
    channel.basicConsume(EXECUTION_ID_1, false, consumer)
}