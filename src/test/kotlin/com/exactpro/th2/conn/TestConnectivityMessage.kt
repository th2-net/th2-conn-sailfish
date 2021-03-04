/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.toJson
import com.exactpro.th2.conn.utility.parentEventID
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestConnectivityMessage {
    @Test
    fun `conversion to raw message`() {
        val connectivityMessage = ConnectivityMessage(
            listOf(
                createMessage(byteArrayOf(42, 43)),
                createMessage(byteArrayOf(44, 45)).apply {
                    metaData.parentEventID = EventID.newBuilder().setId("id").build()
                },
                createMessage(byteArrayOf(46, 47))
            ),
            "test",
            Direction.SECOND,
            1L
        )

        val rawMessage: RawMessage = connectivityMessage.convertToProtoRawMessage()
        assertArrayEquals(byteArrayOf(42, 43, 44, 45, 46, 47), rawMessage.body.toByteArray())  { "RawMessage: " + rawMessage.toJson() }
        assertEquals(EventID.newBuilder().setId("id").build(), rawMessage.parentEventId)  { "RawMessage: " + rawMessage.toJson() }
        with(rawMessage.metadata.id) {
            assertEquals(1L, sequence) { "RawMessage: " + rawMessage.toJson() }
            assertEquals(Direction.SECOND, direction)  { "RawMessage: " + rawMessage.toJson() }
        }
    }

    private fun createMessage(body: ByteArray): IMessage {
        return DefaultMessageFactory.getFactory().createMessage("Test", "test").apply {
            metaData.rawMessage = body
        }
    }
}