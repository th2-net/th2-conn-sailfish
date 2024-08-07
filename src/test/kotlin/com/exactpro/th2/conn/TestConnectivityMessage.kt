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
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.conn.saver.impl.ProtoMessageSaver
import com.exactpro.th2.conn.utility.parentEventID
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.`when`

class TestConnectivityMessage {
    @Test
    fun `conversion to raw message`() {
        val commonFactory = Mockito.mock(CommonFactory::class.java)
        `when`(commonFactory.newMessageIDBuilder()).thenCallRealMethod();
        val boxConfiguration = BoxConfiguration()
        `when`(commonFactory.boxConfiguration).thenReturn(boxConfiguration)

        val id = "id"
        val firstMessageBody = byteArrayOf(42, 43)
        val secondMessageBody = byteArrayOf(44, 45)
        val thirdMessageBody = byteArrayOf(46, 47)
        val sessionAlias = "test"
        val direction = Direction.SECOND
        val sequence = 1L
        val connectivityMessage = ConnectivityMessage(
            listOf(
                createMessage(firstMessageBody),
                createMessage(secondMessageBody).apply {
                    metaData.parentEventID = EventID.newBuilder().setId(id).build()
                },
                createMessage(thirdMessageBody)
            ),
            commonFactory.newMessageIDBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(sessionAlias))
                .setDirection(direction)
                .setSequence(sequence)
        )

        val rawMessage: RawMessage = ProtoMessageSaver.convertToProtoRawMessage(connectivityMessage)
        val rawMessageAsString = "RawMessage: " + rawMessage.toJson()
        assertArrayEquals(
            firstMessageBody + secondMessageBody + thirdMessageBody,
            rawMessage.body.toByteArray()
        ) { rawMessageAsString }
        assertEquals(EventID.newBuilder().setId(id).build(), rawMessage.parentEventId) { rawMessageAsString }
        with(rawMessage.metadata.id) {
            assertEquals(boxConfiguration.bookName, bookName) { rawMessageAsString }
            assertEquals(sessionAlias, connectionId.sessionAlias) { rawMessageAsString }
            assertEquals(direction, direction) { rawMessageAsString }
            assertEquals(sequence, sequence) { rawMessageAsString }
        }
    }

    private fun createMessage(body: ByteArray): IMessage {
        return DefaultMessageFactory.getFactory().createMessage("Test", "test").apply {
            metaData.rawMessage = body
        }
    }
}