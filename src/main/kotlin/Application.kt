package com.aarokoinsaari

import com.aarokoinsaari.api.ingestionRoutes
import com.aarokoinsaari.core.IngestionService
import com.aarokoinsaari.core.RedisRepository
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.routing
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import org.slf4j.event.Level

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.module() {
    install(ContentNegotiation) {
        json()
    }
    install(CallLogging) {
        level = Level.INFO
    }
    install(StatusPages) {
        exception<Throwable> { call, cause ->
            call.respondText(
                text = "500: ${cause.message}",
                status = HttpStatusCode.InternalServerError
            )
        }
    }

    val redisUrl = System.getenv("REDIS_URL")
        ?: environment.config.propertyOrNull("redis.url")?.getString()
        ?: "redis://localhost:6379"
    val archivePath = System.getenv("ARCHIVE_PATH")
        ?: environment.config.propertyOrNull("ingestion.archivePath")?.getString()
        ?: "enron_mail_20150507.tar.gz"
    val repository = RedisRepository(redisUrl)
    val scope = CoroutineScope(SupervisorJob())
    val ingestionService = IngestionService(repository, scope, archivePath)

    routing {
        ingestionRoutes(repository, ingestionService)
    }
}
