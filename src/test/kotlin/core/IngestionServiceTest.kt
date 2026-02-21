package com.aarokoinsaari.core

import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class IngestionServiceTest {

    companion object {
        @Container
        val redis = GenericContainer("redis:alpine").withExposedPorts(6379)
    }

    private lateinit var repository: RedisRepository
    private lateinit var scope: CoroutineScope

    @BeforeEach
    fun setup() {
        val url = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        repository = RedisRepository(url)
        repository.resetAll()
        scope = CoroutineScope(SupervisorJob())
    }

    @AfterEach
    fun teardown() {
        scope.cancel()
        repository.close()
    }

    @Test
    fun `start returns false when ingestion is already running`() {
        repository.setStatus(RedisRepository.STATUS_RUNNING)

        val service = IngestionService(
            repository = repository,
            scope = scope,
            archivePath = ""
        )

        assertFalse(service.start())
    }

    @Test
    fun `start returns true when status is idle`() {
        val service = IngestionService(
            repository = repository,
            scope = scope,
            archivePath = "nonexistent.tar.gz"
        )

        assertTrue(service.start())
    }

    @Test
    fun `start returns true when status is finished`() {
        repository.setStatus(RedisRepository.STATUS_FINISHED)

        val service = IngestionService(
            repository = repository,
            scope = scope,
            archivePath = "nonexistent.tar.gz"
        )

        assertTrue(service.start())
    }

    @Test
    fun `start returns true when status is failed`() {
        repository.setStatus(RedisRepository.STATUS_FAILED)

        val service = IngestionService(
            repository = repository,
            scope = scope,
            archivePath = "nonexistent.tar.gz"
        )

        assertTrue(service.start())
    }

    @Test
    fun `sets status to failed when archive file does not exist`() {
        val service = IngestionService(
            repository = repository,
            scope = scope,
            archivePath = "/nonexistent/path/fake.tar.gz"
        )

        service.start()
        awaitStatus(RedisRepository.STATUS_FAILED)

        assertEquals(RedisRepository.STATUS_FAILED, repository.getStatus())
    }

    @Test
    fun `sets status to failed when file is not a valid tar gz`() {
        val tempFile = File.createTempFile("not-a-tar", ".tar.gz")
        tempFile.writeText("this is not a tar.gz file")
        tempFile.deleteOnExit()

        val service = IngestionService(
            repository = repository,
            scope = scope,
            archivePath = tempFile.absolutePath
        )

        service.start()
        awaitStatus(RedisRepository.STATUS_FAILED)

        assertEquals(RedisRepository.STATUS_FAILED, repository.getStatus())
    }

    @Test
    fun `resets state before starting new ingestion`() {
        repository.flushBatch(listOf("old@enron.com", "old@enron.com"))
        repository.setStatus(RedisRepository.STATUS_FINISHED)

        val service = IngestionService(
            repository = repository,
            scope = scope,
            archivePath = "/nonexistent/fake.tar.gz"
        )

        service.start()
        awaitStatus(RedisRepository.STATUS_FAILED)

        assertEquals(0L, repository.getTotalMessages())
        assertEquals(emptyList(), repository.getTopSenders(10))
    }

    private fun awaitStatus(expected: String, timeoutMs: Long = 5000) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (repository.getStatus() == expected) return
            Thread.sleep(50)
        }
        assertEquals(expected, repository.getStatus(), "Timed out waiting for status '$expected'")
    }
}
