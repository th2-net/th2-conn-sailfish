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

import com.exactpro.sf.common.services.ServiceName;
import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.sf.services.ServiceEvent;
import com.exactpro.sf.services.ServiceEventFactory;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.conn.events.EventDispatcher;
import com.exactpro.th2.conn.events.EventHolder;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;

public class TestServiceListener {


    @Test
    public void onEventTest() throws JsonProcessingException {

        FlowableProcessor<ConnectivityMessage> processor = UnicastProcessor.create();
        MyEventDispatcher eventDispatcher = new MyEventDispatcher();

        ServiceListener serviceListener = new ServiceListener(Map.of(Direction.FIRST, new AtomicLong(1)),
                "SessionAlias", processor, eventDispatcher);

        ServiceEvent serviceEvent = ServiceEventFactory.createEventInfo(ServiceName.parse("serviceName"), ServiceEvent.Type.INFO,
                "Warn: incoming message with missing field: 45: Required " +
                        "tag missing, field=45: 8=FIXT.1.1\0019=112\00135=j\00134=3783\00149=FGW" +
                        "\00152=20210203-12:30:48.238\00156=DEMO-CONN1\00158=Unknown SecurityID" +
                        "\001371=48\001372=D\001379=9741113\001380=2\00110=019\001", null);

        IServiceProxy serviceProxy = mock(IServiceProxy.class);
        serviceListener.onEvent(serviceProxy, serviceEvent);

        Event event = eventDispatcher.getEvent();
        com.exactpro.th2.common.grpc.Event grpcEvent = event.toProto(null);

        String name = grpcEvent.getName();
        Assertions.assertEquals("Service [serviceName] emitted event with status INFO", name);

        String body = grpcEvent.getBody().toStringUtf8();
        Assertions.assertEquals("[{\"data\":\"Warn: incoming message with missing field: 45: Required " +
                "tag missing, field=45: 8=FIXT.1.1\\u00019=112\\u000135=j\\u000134=3783\\u000149=FGW" +
                "\\u000152=20210203-12:30:48.238\\u000156=DEMO-CONN1\\u000158=Unknown SecurityID" +
                "\\u0001371=48\\u0001372=D\\u0001379=9741113\\u0001380=2\\u000110=019\\u0001\",\"type\":\"message\"}]", body);
    }


    public static class MyEventDispatcher implements EventDispatcher {

        Event event;

        @Override
        public void store(@NotNull EventHolder eventHolder) {
            this.event = eventHolder.getEvent();
        }

        @Override
        public void store(@NotNull Event event, @NotNull String parentId) {
            this.event = event;
        }

        public Event getEvent() {
            return event;
        }
    }
}
