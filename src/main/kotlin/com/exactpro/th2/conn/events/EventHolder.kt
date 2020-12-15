package com.exactpro.th2.conn.events

import com.exactpro.th2.common.event.Event

class EventHolder(
    val type: EventType,
    val event: Event
) {
    companion object {
        @JvmStatic
        fun createError(event: Event): EventHolder = EventHolder(EventType.ERROR, event)

        @JvmStatic
        fun createServiceEvent(event: Event): EventHolder = EventHolder(EventType.SERVICE_EVENT, event)
    }
}