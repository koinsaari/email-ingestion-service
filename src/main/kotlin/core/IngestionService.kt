package com.aarokoinsaari.core

import kotlinx.coroutines.CoroutineScope

class IngestionService(
    private val repository: RedisRepository,
    private val scope: CoroutineScope
) {

    fun start(): Boolean {
        if (repository.getStatus() == RedisRepository.STATUS_RUNNING) {
            return false
        }

        repository.resetAll()
        repository.setStatus(RedisRepository.STATUS_RUNNING)

        // TODO: launch streaming

        repository.setStatus(RedisRepository.STATUS_FINISHED)
        return true
    }
}
