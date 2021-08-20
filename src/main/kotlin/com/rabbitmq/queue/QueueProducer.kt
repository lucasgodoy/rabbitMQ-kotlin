package com.rabbitmq.queue

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.queue.QueueProducer.Companion.EXECUTION_ID_1
import com.rabbitmq.queue.QueueProducer.Companion.EXECUTION_ID_2

class QueueProducer {
    companion object {
        const val EXECUTION_ID_1 = "default_consumer_1"
        const val EXECUTION_ID_2 = "default_consumer_2"
    }
}

private fun sendTask(executionId: String, numberOfTasks: Short, channel: Channel) {
    for (i in 1..numberOfTasks) {
        val task = "exec_${executionId}_task_$i"
        channel.queueDeclare(executionId, true, false, true, null)
        channel.basicPublish(
            "", // default exchange which is a generic direct exchange
            executionId,
            MessageProperties.PERSISTENT_TEXT_PLAIN,
            task.toByteArray(charset("UTF-8"))
        )
        println(" [x] Sent with routing '$executionId' : '$task'")
    }
}

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->

        val channel = connection.createChannel()

        sendTask(executionId = EXECUTION_ID_1, numberOfTasks = 10, channel = channel)
        sendTask(executionId = EXECUTION_ID_2, numberOfTasks = 20, channel = channel)
    }
}