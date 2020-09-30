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
@file:JvmName("SailfishMetadataExtensions")
package com.exactpro.th2.conn.utility

import com.exactpro.sf.common.messages.IMetadata
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.conn.utility.MetadataProperty.PARENT_EVENT_ID
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceBlockingStub
import com.exactpro.th2.eventstore.grpc.StoreEventRequest
import com.exactpro.th2.infra.grpc.EventID
import com.fasterxml.jackson.core.JsonProcessingException
import org.slf4j.LoggerFactory

@Suppress("UNCHECKED_CAST")
private fun <T> IMetadata.getAs(property: MetadataProperty): T? = get(property.propertyName) as T?

private fun <T : Any> IMetadata.getRequired(property: MetadataProperty): T = checkNotNull(getAs(property)) { "${property.propertyName} is not set" }

private fun IMetadata.setOnce(property: MetadataProperty, value: Any) {
    val propertyName = property.propertyName
    check(!contains(propertyName)) { "$propertyName is already set" }
    set(propertyName, value)
}

fun IMetadata.contains(property: MetadataProperty) : Boolean {
    val propertyName = property.propertyName
    return contains(propertyName)
}

var IMetadata.parentEventID: EventID
    get() = getRequired(PARENT_EVENT_ID)
    set(value) = setOnce(PARENT_EVENT_ID, value)