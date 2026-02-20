package com.aarokoinsaari

import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals

class ApplicationTest {

    @Test
    fun testModuleLoads() = testApplication {
        application {
            module()
        }
        client.get("/status").apply {
            assertEquals(HttpStatusCode.NotFound, status)
        }
    }
}
