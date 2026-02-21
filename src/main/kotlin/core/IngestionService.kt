package com.aarokoinsaari.core

import jakarta.mail.Session
import jakarta.mail.internet.InternetAddress
import jakarta.mail.internet.MimeMessage
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.FileInputStream
import java.util.Properties
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.slf4j.LoggerFactory

class IngestionService(
    private val repository: RedisRepository,
    private val scope: CoroutineScope,
    private val archivePath: String
) {

    private val logger = LoggerFactory.getLogger(IngestionService::class.java)
    private val mailSession = Session.getDefaultInstance(Properties())

    companion object {
        const val CHANNEL_CAPACITY = 1000
        const val WORKER_COUNT = 10
        const val BATCH_SIZE = 500
        const val MAX_ENTRY_SIZE = 10 * 1024 * 1024L // 10MB
    }

    fun start(): Boolean {
        if (repository.getStatus() == RedisRepository.STATUS_RUNNING) {
            return false
        }

        repository.resetAll()
        repository.setStatus(RedisRepository.STATUS_RUNNING)

        val channel = Channel<String>(capacity = CHANNEL_CAPACITY)

        scope.launch(Dispatchers.IO) {
            try {
                launch {
                    streamArchive(channel)
                }

                val workerJobs = List(WORKER_COUNT) {
                    launch(Dispatchers.Default) {
                        val batch = mutableListOf<String>()

                        for (rawEmail in channel) {
                            val sender = parseSender(rawEmail) ?: continue
                            batch.add(sender)

                            if (batch.size >= BATCH_SIZE) {
                                repository.flushBatch(batch)
                                batch.clear()
                            }
                        }

                        if (batch.isNotEmpty()) {
                            repository.flushBatch(batch)
                        }
                    }
                }

                workerJobs.joinAll()
                repository.setStatus(RedisRepository.STATUS_FINISHED)
                logger.info("Ingestion finished. Total: ${repository.getTotalMessages()}")
            } catch (e: Exception) {
                logger.error("Ingestion failed", e)
                repository.setStatus(RedisRepository.STATUS_FAILED)
            }
        }

        return true
    }

    private suspend fun streamArchive(channel: Channel<String>) {
        try {
            FileInputStream(archivePath).use { fileStream ->
                BufferedInputStream(fileStream).use { buffered ->
                    GzipCompressorInputStream(buffered).use { gzip ->
                        TarArchiveInputStream(gzip).use { tar ->
                            var entry = tar.nextEntry
                            while (entry != null) {
                                if (!entry.isDirectory && entry.size <= MAX_ENTRY_SIZE) {
                                    val bytes = tar.readBytes()
                                    channel.send(String(bytes))
                                } else if (entry.size > MAX_ENTRY_SIZE) {
                                    logger.warn("Skipping oversized entry: ${entry.name} (${entry.size} bytes)")
                                }
                                entry = tar.nextEntry
                            }
                        }
                    }
                }
            }
        } finally {
            channel.close()
        }
    }

    private fun parseSender(rawEmail: String): String? =
        try {
            val message = MimeMessage(mailSession, ByteArrayInputStream(rawEmail.toByteArray()))
            val from = message.from?.firstOrNull() as? InternetAddress
            from?.address?.lowercase()
        } catch (e: Exception) {
            null // TODO
        }
}
