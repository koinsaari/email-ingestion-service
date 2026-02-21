package com.aarokoinsaari.core

import kotlin.test.assertEquals
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class RedisRepositoryTest {

    companion object {
        @Container
        val redis = GenericContainer("redis:alpine").withExposedPorts(6379)
    }

    private lateinit var repository: RedisRepository

    @BeforeEach
    fun setup() {
        val url = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        repository = RedisRepository(url)
        repository.resetAll()
    }

    @AfterEach
    fun teardown() {
        repository.close()
    }

    @Test
    fun `getStatus returns idle when no status is set`() {
        assertEquals(RedisRepository.STATUS_IDLE, repository.getStatus())
    }

    @Test
    fun `setStatus and getStatus round-trip`() {
        repository.setStatus(RedisRepository.STATUS_RUNNING)
        assertEquals(RedisRepository.STATUS_RUNNING, repository.getStatus())

        repository.setStatus(RedisRepository.STATUS_FINISHED)
        assertEquals(RedisRepository.STATUS_FINISHED, repository.getStatus())
    }

    @Test
    fun `getTotalMessages returns 0 when empty`() {
        assertEquals(0L, repository.getTotalMessages())
    }

    @Test
    fun `flushBatch increments total messages count`() {
        repository.flushBatch(listOf("alice@enron.com", "bob@enron.com", "alice@enron.com"))
        assertEquals(3L, repository.getTotalMessages())
    }

    @Test
    fun `flushBatch tracks sender scores in sorted set`() {
        repository.flushBatch(listOf(
            "alice@enron.com",
            "alice@enron.com",
            "alice@enron.com",
            "bob@enron.com",
            "bob@enron.com",
            "carol@enron.com"
        ))

        val topSenders = repository.getTopSenders(10)
        assertEquals(3, topSenders.size)
        assertEquals("alice@enron.com", topSenders[0].first)
        assertEquals(3.0, topSenders[0].second)
        assertEquals("bob@enron.com", topSenders[1].first)
        assertEquals(2.0, topSenders[1].second)
        assertEquals("carol@enron.com", topSenders[2].first)
        assertEquals(1.0, topSenders[2].second)
    }

    @Test
    fun `getTopSenders respects limit`() {
        repository.flushBatch(listOf("a@enron.com", "b@enron.com", "c@enron.com"))

        val topSenders = repository.getTopSenders(2)
        assertEquals(2, topSenders.size)
    }

    @Test
    fun `multiple flushBatch calls accumulate correctly`() {
        repository.flushBatch(listOf("alice@enron.com", "bob@enron.com"))
        repository.flushBatch(listOf("alice@enron.com", "carol@enron.com"))

        assertEquals(4L, repository.getTotalMessages())

        val topSenders = repository.getTopSenders(10)
        assertEquals("alice@enron.com", topSenders[0].first)
        assertEquals(2.0, topSenders[0].second)
    }

    @Test
    fun `flushBatch with empty list does not change state`() {
        repository.flushBatch(emptyList())

        assertEquals(0L, repository.getTotalMessages())
        assertEquals(emptyList(), repository.getTopSenders(10))
    }

    @Test
    fun `flushBatch handles duplicate sender in single batch`() {
        repository.flushBatch(listOf(
            "same@enron.com",
            "same@enron.com",
            "same@enron.com",
            "same@enron.com",
            "same@enron.com"
        ))

        val topSenders = repository.getTopSenders(10)
        assertEquals(1, topSenders.size)
        assertEquals("same@enron.com", topSenders[0].first)
        assertEquals(5.0, topSenders[0].second)
        assertEquals(5L, repository.getTotalMessages())
    }

    @Test
    fun `flushBatch handles special characters in email addresses`() {
        val specialEmails = listOf(
            "user+tag@enron.com",
            "first.last@enron.com",
            "\"quoted\"@enron.com"
        )
        repository.flushBatch(specialEmails)

        assertEquals(3L, repository.getTotalMessages())
        val topSenders = repository.getTopSenders(10)
        assertEquals(3, topSenders.size)
    }

    @Test
    fun `getTopSenders returns empty list when no data`() {
        assertEquals(emptyList(), repository.getTopSenders(10))
    }

    @Test
    fun `resetAll clears all keys`() {
        repository.setStatus(RedisRepository.STATUS_RUNNING)
        repository.flushBatch(listOf("alice@enron.com"))

        repository.resetAll()

        assertEquals(RedisRepository.STATUS_IDLE, repository.getStatus())
        assertEquals(0L, repository.getTotalMessages())
        assertEquals(emptyList(), repository.getTopSenders(10))
    }
}
