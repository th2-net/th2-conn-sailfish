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
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.exactpro.th2.conn.events.EventType;
import com.google.protobuf.ByteString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TestEvent {

    private static IServiceProxy serviceProxy;
    private static MyEventDispatcher eventDispatcher;
    private static MessageSender messageSender;
    private static MessageListener messageListener;

    @BeforeAll
    public static void initMessages(){
        serviceProxy = mock(IServiceProxy.class);
        MessageRouter<RawMessageBatch> router = mock(MessageRouter.class);
        MessageRouter<EventBatch> eventBatchMessageRouter = mock(MessageRouter.class);

        doAnswer(invocation -> {
            messageListener = invocation.getArgument(0);
            return (SubscriberMonitor) () -> {
            };
        }).when(router).subscribeAll(any(), any());

        eventDispatcher = new MyEventDispatcher(eventBatchMessageRouter, "rootID",
                Map.of(EventType.ERROR, "rootEventID"));

        messageSender = new MessageSender(serviceProxy, router, eventDispatcher,
                EventID.newBuilder().setId("stubID").build());
        messageSender.start();
    }

    public void sendIncorrectMessage() throws Exception {
        RawMessageBatch rawMessageBatch = RawMessageBatch.newBuilder()
                .addMessages(RawMessage.newBuilder().build())
                .build();

        doThrow(new IllegalStateException("error")).when(serviceProxy).sendRaw(any(), any());
        messageListener.handler("stubValue", rawMessageBatch);
    }

    @Test
    public void eventHasBodyTest() throws Exception {
        sendIncorrectMessage();
        Event event = eventDispatcher.getEvents().get(0);
        event.addSubEvent(Event.start());

        ByteString body = event.toProto(EventUtils.toEventID(eventDispatcher.getParentIds().get(0))).getBody();
        Assertions.assertEquals("[{\"data\":\"java.lang.IllegalStateException: error\",\"type\":\"message\"}," +
                "{\"data\":\"Cannot send message. Message body in base64:\",\"type\":\"message\"},{\"data\":\"\"," +
                "\"type\":\"message\"},{\"data\":\"java.lang.IllegalStateException: error\",\"type\":\"message\"}]", body.toStringUtf8());
    }

    @Test
    public void eventHasNameTest() throws Exception {
        sendIncorrectMessage();
        Event event = eventDispatcher.getEvents().get(0);
        event.addSubEvent(Event.start());

        String name = event.toProto(EventUtils.toEventID(eventDispatcher.getParentIds().get(0))).getName();
        Assertions.assertEquals("Raw message sending error", name);
    }

    @Test
    public void sentMessageWithParentEventIDTest() throws Exception {
        RawMessageBatch rawMessageBatch = RawMessageBatch.newBuilder()
                .addMessages(RawMessage.newBuilder()
                        .setParentEventId(EventID.newBuilder()
                                .setId("RawMessageParentEventID")).build())
                .build();

        doThrow(new IllegalStateException("error")).when(serviceProxy).sendRaw(any(), any());
        messageListener.handler("stubValue", rawMessageBatch);

        Event event = eventDispatcher.getEvents().get(0);
        event.addSubEvent(Event.start());

        EventBatch eventBatch = event.toBatchProto(EventUtils.toEventID(eventDispatcher.getParentIds().get(0)));
        Assertions.assertEquals("RawMessageParentEventID", eventBatch.getParentEventId().getId());
    }

    @Test
    public void sentMessageWithoutParentEventIDTest() throws Exception {
        sendIncorrectMessage();
        Event event2 = eventDispatcher.getEvents().get(0);
        event2.addSubEvent(Event.start());

        EventBatch eventBatch2 = event2.toBatchProto(EventUtils.toEventID(eventDispatcher.getParentIds().get(0)));
        Assertions.assertEquals("rootEventID", eventBatch2.getParentEventId().getId());
    }

    @AfterAll
    private static void close() throws IOException {
        messageSender.stop();
    }


    public static class MyEventDispatcher implements EventDispatcher {

        MessageRouter<EventBatch> messageRouter;
        String rootID;
        Map<EventType, String> parentIdByType;

        List<Event> events = new ArrayList<>();
        List<String> parentIds = new ArrayList<>();

        public MyEventDispatcher(MessageRouter<EventBatch> messageRouter, String rootID,
                                 Map<EventType, String> parentIdByType) {
            this.messageRouter = messageRouter;
            this.rootID = rootID;
            this.parentIdByType = parentIdByType;
        }

        @Override
        public void store(@NotNull EventHolder eventHolder) {
            eventDispatcher.parentIds.clear();
            eventDispatcher.events.clear();

            events.add(eventHolder.getEvent());
            parentIds.add(parentIdByType.get(EventType.ERROR) != null ? parentIdByType.get(EventType.ERROR) : rootID);
        }

        @Override
        public void store(@NotNull Event event, @NotNull String parentId) {
            events.add(event);
            parentIds.add(parentId);

        }

        public List<Event> getEvents() {
            return events;
        }

        public List<String> getParentIds() {
            return parentIds;
        }
    }
}