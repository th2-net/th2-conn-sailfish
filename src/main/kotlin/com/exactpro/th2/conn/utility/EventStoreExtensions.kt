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
package com.exactpro.th2.conn.utility

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.EventUtils.toEventID
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.MessageRouter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.util.JsonFormat
import mu.KotlinLogging

private val LOGGER = KotlinLogging.logger { }

@JvmOverloads
@Throws(JsonProcessingException::class)
fun MessageRouter<EventBatch>.storeEvent(event: Event, parentEventID: String? = null) : Event {
    try {
        send(EventBatch.newBuilder().addEvents(event.toProto(toEventID(parentEventID))).build())
    } catch (e: Exception) {
        throw RuntimeException("Event '" + event.id + "' store failure", e)
    }
    LOGGER.debug("Event {} sent", event.id)
    return event
}

/**
 * @param parentEventID the ID of the root parent that all events should be attached.
 *                      If `null` the events will be stored as a root events (without attaching to any parent).
 * @param events events to store
 */
@JvmOverloads
@Throws(JsonProcessingException::class)
fun MessageRouter<EventBatch>.storeEvents(parentEventID: String? = null, vararg events: Event) {
    try {
        val batchBuilder = EventBatch.newBuilder().apply {
            if (parentEventID != null) {
                setParentEventId(EventID.newBuilder().setId(parentEventID))
            }
            val parentId = toEventID(parentEventID)
            for (event in events) {
                addEvents(event.toProto(parentId))
            }
        }
        send(batchBuilder.build())
    } catch (e: Exception) {
        throw RuntimeException("Events '" + events.map { it.id } + "' store failure", e)
    }
    LOGGER.debug("Events {} sent", events.map { it.id })
}

// TODO: maybe we should move it to common library
fun Event.addException(t: Throwable) {
    var error: Throwable? = t
    do {
        bodyData(EventUtils.createMessageBean(error?.toString()))
        error = error?.cause
    } while (error != null)
}

// TODO: probably we should move it to common library
fun createProtoMessageBean(msg: MessageOrBuilder): IBodyData {
    return ProtoMessageData(msg)
}

@JsonSerialize(using = ProtoMessageSerializer::class)
class ProtoMessageData(
    @JsonIgnore
    val messageOrBuilder: MessageOrBuilder
) : IBodyData

class ProtoMessageSerializer : StdSerializer<ProtoMessageData>(ProtoMessageData::class.java) {
    override fun serialize(value: ProtoMessageData, gen: JsonGenerator, provider: SerializerProvider) {
        gen.writeRawValue(JsonFormat.printer().print(value.messageOrBuilder))
    }
}