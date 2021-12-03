/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn.events

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.conn.events.impl.EventDispatcherImpl
import java.io.IOException

interface EventDispatcher {
    @Throws(IOException::class)
    fun store(eventHolder: EventHolder)

    @Throws(IOException::class)
    fun store(event: Event, parentId: EventID)

    companion object {
        @JvmStatic
        fun createDispatcher(
            router: MessageRouter<EventBatch>,
            rootId: EventID,
            parentIdByType: Map<EventType, EventID>
        ): EventDispatcher = EventDispatcherImpl(router, rootId, parentIdByType)
    }
}