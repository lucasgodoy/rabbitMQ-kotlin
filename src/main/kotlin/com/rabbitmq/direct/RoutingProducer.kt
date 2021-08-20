package com.rabbitmq.direct

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.direct.RoutingProducer.Companion.EXCHANGE_NAME
import com.rabbitmq.direct.RoutingProducer.Companion.EXECUTION_ID_1
import com.rabbitmq.direct.RoutingProducer.Companion.EXECUTION_ID_2

class RoutingProducer {
    companion object {
        const val EXCHANGE_NAME = "tasks_routing"
        const val EXECUTION_ID_1 = "routing_consumer_1"
        const val EXECUTION_ID_2 = "routing_consumer_2"
    }
}

private fun sendTask(executionId: String, numberOfTasks: Short, channel: Channel) {
    channel.queueDeclare(executionId, true, false, true, null)
    channel.queueBind(executionId, EXCHANGE_NAME, executionId)
    for (i in 1..numberOfTasks) {
        val task = "task_$i"
        channel.basicPublish(
            EXCHANGE_NAME,
            executionId,
            MessageProperties.PERSISTENT_TEXT_PLAIN,
            task.toByteArray(charset("UTF-8"))
        )
        println(" [x] Sent with rout '$executionId' : '$task'")
    }
}

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->

        val channel = connection.createChannel()
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, null)

        sendTask(executionId = EXECUTION_ID_1, numberOfTasks = 10, channel = channel)
        sendTask(executionId = EXECUTION_ID_2, numberOfTasks = 20, channel = channel)
    }
}