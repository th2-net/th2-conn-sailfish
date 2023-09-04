/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Map;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.MessageListener;

import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.conn.events.EventDispatcher;
import org.jetbrains.annotations.Nullable;

public class ProtoMessageSender extends AbstractMessageSender implements MessageListener<RawMessageBatch> {
    private final MessageRouter<RawMessageBatch> router;
    private volatile SubscriberMonitor subscriberMonitor;

    public ProtoMessageSender(IServiceProxy serviceProxy,
                              MessageRouter<RawMessageBatch> router,
                              EventDispatcher eventDispatcher,
                              EventID untrackedMessagesRoot) {
        super(serviceProxy, eventDispatcher, untrackedMessagesRoot);
        this.router = requireNonNull(router, "Message router can't be null");
    }

    @Override
    public void start() {
        if (subscriberMonitor != null) {
            throw new IllegalStateException("Already subscribe");
        }

        logger.info("Subscribing to proto queue with messages to send");

        subscriberMonitor = router.subscribeAll(this, SEND_ATTRIBUTE, QueueAttribute.RAW.getValue());
    }

    public void stop() throws IOException {
        if (subscriberMonitor == null) {
            throw new IllegalStateException("Not yet start subscribe");
        }

        logger.info("Stop listener the 'sender' proto queue");
        try {
            subscriberMonitor.unsubscribe();
        } catch (Exception e) {
            logger.error("Can not unsubscribe", e);
        }
    }

    @Override
    public void handle(DeliveryMetadata deliveryMetadata, RawMessageBatch message) {
        for (RawMessage protoMessage : message.getMessagesList()) {
            try {
                sendMessage(new ProtoHolder(protoMessage));
            } catch (InterruptedException e) {
                logger.error("Send message operation interrupted. Consumer tag {}", deliveryMetadata.getConsumerTag(), e);
            } catch (Exception e) {
                logger.error("Could not send IMessage. Consumer tag {}", deliveryMetadata.getConsumerTag(), e);
            }
        }
    }

    @Override
    public void onClose() {}

    private static class ProtoHolder implements MessageHolder {
        private final RawMessage proto;

        private ProtoHolder(RawMessage proto) {
            this.proto = requireNonNull(proto, "proto raw message");
        }

        @Nullable
        @Override
        public EventID getParentEventId() {
            return proto.hasParentEventId()
                    ? proto.getParentEventId()
                    : null;
        }

        @Override
        public MessageID getId() {
            return proto.getMetadata().getId();
        }

        @Override
        public Map<String, String> getProperties() {
            return proto.getMetadata().getPropertiesMap();
        }

        @Override
        public byte[] getBody() {
            return proto.getBody().toByteArray();
        }
    }

}
