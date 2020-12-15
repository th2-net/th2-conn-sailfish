package com.exactpro.th2.conn.events

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.conn.events.impl.EventDispatcherImpl
import java.io.IOException

interface EventDispatcher {
    @Throws(IOException::class)
    fun store(eventHolder: EventHolder)

    @Throws(IOException::class)
    fun store(event: Event, parentId: String)

    companion object {
        @JvmStatic
        fun createDispatcher(
            router: MessageRouter<EventBatch>,
            rootID: String,
            parentIdByType: Map<EventType, String>
        ): EventDispatcher = EventDispatcherImpl(router, rootID, parentIdByType)
    }
}