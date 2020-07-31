/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@file:JvmName("EventStoreExtensions")
package com.exactpro.th2.connectivity.utility

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceBlockingStub
import com.exactpro.th2.eventstore.grpc.StoreEventRequest
import com.fasterxml.jackson.core.JsonProcessingException
import org.slf4j.LoggerFactory

// FIXME: incorrect definition
private val LOGGER = LoggerFactory.getLogger("com.exactpro.th2.connectivity.utility.EventStoreExtensions")

@JvmOverloads
@Throws(JsonProcessingException::class)
fun EventStoreServiceBlockingStub.storeEvent(event: Event, parentEventID: String? = null) {
    val response = storeEvent(StoreEventRequest.newBuilder()
        .setEvent(event.toProtoEvent(parentEventID))
        .build())
    if (response.hasError()) {
        throw RuntimeException("Evnet '" + event.id + "' store failure, reason: " + response.error.value)
    }
    LOGGER.debug("Event '{}' sent", event.id)
}