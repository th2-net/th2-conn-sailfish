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
package com.exactpro.th2.conn;

import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.exactpro.th2.conn.events.EventType;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TestEvent {

    private static final String BOOK_NAME = "book";
    private static final EventID ROOT_ID = EventUtils.toEventID(Instant.now(), BOOK_NAME, "rootID");
    private static final Map<EventType, EventID> PARENT_IDS = Map.of(EventType.ERROR, EventUtils.toEventID(Instant.now(), BOOK_NAME, "errorEventID"));

    private static IServiceProxy serviceProxy;
    private static EventDispatcher eventDispatcher;
    private static MessageSender messageSender;
    private static MessageListener<RawMessageBatch> messageListener;
    private static Event event;
    private static EventID parentId;

    @BeforeAll
    static void initMessages() throws IOException {
        serviceProxy = mock(IServiceProxy.class);
        @SuppressWarnings("unchecked")
        MessageRouter<RawMessageBatch> router = mock(MessageRouter.class);

        doAnswer(invocation -> {
            messageListener = invocation.getArgument(0);
            return (SubscriberMonitor) () -> { };
        }).when(router).subscribeAll(any(), any());

        eventDispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            EventHolder eventHolder = invocation.getArgument(0);
            EventType eventType = eventHolder.getType();

            eventDispatcher.store(eventHolder.getEvent(),
                    PARENT_IDS.get(eventType) == null ? ROOT_ID : PARENT_IDS.get(eventType));
            return null;
        }).when(eventDispatcher).store(any());

        doAnswer(invocation -> {
            event = invocation.getArgument(0);
            parentId = invocation.getArgument(1);
            return null;
        }).when(eventDispatcher).store(any(), any());

        messageSender = new MessageSender(serviceProxy, router, eventDispatcher,
                EventID.newBuilder().setId("stubID").build());
        messageSender.start();
    }

    @AfterEach
    void clear() {
        event = null;
        parentId = null;
    }

    public void sendIncorrectMessage() throws Exception {
        RawMessageBatch rawMessageBatch = RawMessageBatch.newBuilder()
                .addMessages(RawMessage.newBuilder().build())
                .build();

        doThrow(new IllegalStateException("error")).when(serviceProxy).sendRaw(any(), any());
        messageListener.handle(new DeliveryMetadata("stubValue", false), rawMessageBatch);
    }

    @Test
    public void eventHasBodyTest() throws Exception {
        sendIncorrectMessage();

        ByteString body = event.toProto(parentId).getBody();
        Assertions.assertEquals("[{\"data\":\"java.lang.IllegalStateException: error\",\"type\":\"message\"}," +
                "{\"data\":\"Cannot send message. Message body in base64:\",\"type\":\"message\"},{\"data\":\"\"," +
                "\"type\":\"message\"},{\"data\":\"java.lang.IllegalStateException: error\",\"type\":\"message\"}]", body.toStringUtf8());
    }

    @Test
    public void eventHasNameTest() throws Exception {
        sendIncorrectMessage();

        String name = event.toProto(parentId).getName();
        Assertions.assertEquals("Failed to send raw message", name);
    }

    @Test
    public void sentMessageWithParentEventIDTest() throws Exception {
        RawMessageBatch rawMessageBatch = RawMessageBatch.newBuilder()
                .addMessages(RawMessage.newBuilder()
                        .setParentEventId(EventUtils.toEventID(Instant.now(), BOOK_NAME, "RawMessageParentEventID"))
                ).build();

        doThrow(new IllegalStateException("error")).when(serviceProxy).sendRaw(any(), any());
        messageListener.handle(new DeliveryMetadata("stubValue", false), rawMessageBatch);

        event.addSubEvent(Event.start());

        EventBatch eventBatch = event.toBatchProto(parentId);
        Assertions.assertEquals("RawMessageParentEventID", eventBatch.getParentEventId().getId());
    }

    @Test
    public void sentMessageWithoutParentEventIDTest() throws Exception {
        sendIncorrectMessage();
        event.addSubEvent(Event.start());

        EventBatch eventBatch = event.toBatchProto(parentId);
        Assertions.assertEquals("errorEventID", eventBatch.getParentEventId().getId());
    }

    @AfterAll
    static void close() throws IOException {
        messageSender.stop();
    }
}