package com.aarokoinsaari.api

import com.aarokoinsaari.core.IngestionService
import com.aarokoinsaari.core.RedisRepository
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post

fun Route.ingestionRoutes(repository: RedisRepository, ingestionService: IngestionService) {

    post("/start") {
        val started = ingestionService.start()
        if (started) {
            call.respond(HttpStatusCode.Accepted, mapOf("message" to "Ingestion started"))
        } else {
            call.respond(HttpStatusCode.Conflict, mapOf("message" to "Ingestion already in progress"))
        }
    }

    get("/status") {
        call.respond(
            StatusResponse(
                status = repository.getStatus(),
                messagesProcessed = repository.getTotalMessages()
            )
        )
    }

    get("/top-senders") {
        val topSenders = repository.getTopSenders().map { (email, count) ->
            TopSenderResponse(email = email, count = count.toLong())
        }
        call.respond(topSenders)
    }
}
