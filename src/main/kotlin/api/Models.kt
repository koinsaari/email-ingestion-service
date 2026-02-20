package com.aarokoinsaari.api

import kotlinx.serialization.Serializable

@Serializable
data class StatusResponse(
    val status: String,
    val messagesProcessed: Long
)

@Serializable
data class TopSenderResponse(
    val email: String,
    val count: Long
)
