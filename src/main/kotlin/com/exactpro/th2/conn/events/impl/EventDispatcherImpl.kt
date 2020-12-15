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