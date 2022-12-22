/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.conn.utility.parentEventID
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.processors.UnicastProcessor
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argThat
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import java.time.Instant

class PipelineTest {

    private val messageFactory = DefaultMessageFactory.getFactory()
    private val eventBatchRouter: MessageRouter<EventBatch> = mock {}
    private val rawMessageRouter: MessageRouter<RawMessageBatch> = mock {}

    @Test
    fun `send event about message sending`() {
        val processor = UnicastProcessor.create<ConnectivityMessage>()
        val idBuilder = MessageID.newBuilder().apply {
            direction = Direction.SECOND
            sequence = 13
        }
        val parentEventId = EventID.newBuilder().apply {
            bookName = "test_book"
            scope = "test_scope"
            startTimestamp = Instant.now().toTimestamp()
            id = "test_id"
        }.build()

        val disposable = MicroserviceMain.createPipeline(
            SCHEDULER,
            processor,
            processor::onComplete,
            eventBatchRouter,
            rawMessageRouter,
            1,
            true
        ).subscribe()
        try {
            val message = ConnectivityMessage(
                listOf(messageFactory.createMessage("test", "ns_test").apply {
                    metaData.parentEventID = parentEventId
                    metaData.rawMessage = ByteArray(10)
                }),
                idBuilder
            )
            processor.onNext(message)
            verify(rawMessageRouter, timeout(1000)
                .times(1)
                .description("Send raw message"))
                    .send(argThat { arg -> arg.messagesCount == 1 })
            verify(eventBatchRouter, timeout(1000)
                .times(1)
                .description("Send event about sending"))
                    .send(argThat { arg -> arg.eventsCount == 1
                            && arg.getEvents(0).attachedMessageIdsCount == 1
                            && arg.getEvents(0).status == EventStatus.SUCCESS
                    })
        } finally {
            disposable.dispose()
        }
    }

    companion object {
        private val SCHEDULER = RxJavaPlugins.createSingleScheduler(
            ThreadFactoryBuilder()
                .setNameFormat("Pipeline-%d").build()
        )

        @AfterAll
        @JvmStatic
        fun afterAll() {
            SCHEDULER.shutdown()
        }
    }
}