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