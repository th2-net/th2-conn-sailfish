package com.exactpro.th2.conn.events.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.conn.events.EventDispatcher
import com.exactpro.th2.conn.events.EventType
import com.exactpro.th2.conn.events.EventHolder
import com.exactpro.th2.conn.utility.storeEvent
import com.fasterxml.jackson.core.JsonProcessingException

class EventDispatcherImpl(
    private val eventBatchRouter: MessageRouter<EventBatch>,
    private val rootID: String,
    private val parentIdByType: Map<EventType, String> = emptyMap()
) : EventDispatcher {
    @Throws(JsonProcessingException::class)
    override fun store(eventHolder: EventHolder) {
        eventBatchRouter.storeEvent(eventHolder.event, parentIdFor(eventHolder.type))
    }

    @Throws(JsonProcessingException::class)
    override fun store(event: Event, parentId: String) {
        eventBatchRouter.storeEvent(event, parentId)
    }

    private fun parentIdFor(type: EventType): String {
        return parentIdByType[type] ?: rootID
    }
}