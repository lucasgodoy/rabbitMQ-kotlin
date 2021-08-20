package com.rabbitmq.topic

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.topic.TopicProducer.Companion.EXCHANGE_NAME

class TopicProducer {
    companion object {
        const val EXCHANGE_NAME = "topics_routing"
        const val TOPIC_1 = "users.*"
        const val TOPIC_2 = "orders.items"
    }
}

private fun sendTask(routingKey: String, numberOfTasks: Short, channel: Channel) {
    for (i in 1..numberOfTasks) {
        val task = "exec_${routingKey}_task_$i"
        channel.basicPublish(
            EXCHANGE_NAME,
            routingKey,
            MessageProperties.PERSISTENT_TEXT_PLAIN,
            task.toByteArray(charset("UTF-8"))
        )
        println(" [x] Sent with routing '$routingKey' : '$task'")
    }
}

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->

        val channel = connection.createChannel()
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true, false, null)

        sendTask(routingKey = "users.create", numberOfTasks = 10, channel = channel)
        sendTask(routingKey = "orders.items", numberOfTasks = 20, channel = channel)
    }
}