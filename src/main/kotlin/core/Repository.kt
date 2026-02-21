package com.aarokoinsaari.core

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection

class RedisRepository(redisUrl: String) : AutoCloseable {

    private val client: RedisClient = RedisClient.create(redisUrl)
    private val connection: StatefulRedisConnection<String, String> = client.connect()
    private val commands = connection.sync()

    companion object {
        const val KEY_STATUS = "ingestion_status"
        const val KEY_TOTAL = "total_messages"
        const val KEY_TOP_SENDERS = "top_senders"

        const val STATUS_IDLE = "idle"
        const val STATUS_RUNNING = "running"
        const val STATUS_FINISHED = "finished"
        const val STATUS_FAILED = "failed"
    }

    fun getStatus(): String =
        commands.get(KEY_STATUS) ?: STATUS_IDLE

    fun setStatus(status: String) {
        commands.set(KEY_STATUS, status)
    }

    fun getTotalMessages(): Long =
        commands.get(KEY_TOTAL)?.toLongOrNull() ?: 0L

    fun getTopSenders(limit: Long = 10): List<Pair<String, Double>> =
        commands.zrevrangeWithScores(KEY_TOP_SENDERS, 0, limit - 1)
            .map { it.value to it.score }

    fun flushBatch(senders: List<String>) {
        val async = connection.async()
        connection.setAutoFlushCommands(false)

        async.incrby(KEY_TOTAL, senders.size.toLong())
        for (sender in senders) {
            async.zincrby(KEY_TOP_SENDERS, 1.0, sender)
        }

        connection.flushCommands()
        connection.setAutoFlushCommands(true)
    }

    fun resetAll() {
        commands.del(KEY_STATUS, KEY_TOTAL, KEY_TOP_SENDERS)
    }

    override fun close() {
        connection.close()
        client.shutdown()
    }
}
