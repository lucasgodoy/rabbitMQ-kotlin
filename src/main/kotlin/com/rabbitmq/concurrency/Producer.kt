package com.rabbitmq.concurrency

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.concurrency.Producer.Companion.QUEUE_ID

class Producer {
    companion object {
        const val QUEUE_ID = "default_multiple_consumers"
    }
}

fun main() {

    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->

        val channel = connection.createChannel()
        channel.queueDeclare(QUEUE_ID, true, false, false, null)
        var i = 0
        while (true) {
            val task = "task_$i"
            channel.basicPublish(
                "", // default exchange which is a generic direct exchange
                QUEUE_ID,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                task.toByteArray(charset("UTF-8"))
            )
            println(" [x] Sent with routing '$i' : '$task'")
            Thread.sleep(2000)
            i++
        }
    }
}