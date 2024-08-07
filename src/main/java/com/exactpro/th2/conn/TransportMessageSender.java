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

import com.exactpro.sf.externalapi.IServiceProxy;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage;
import com.exactpro.th2.common.utils.event.transport.EventUtilsKt;
import com.exactpro.th2.common.utils.message.transport.MessageUtilsKt;
import com.exactpro.th2.conn.events.EventDispatcher;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TransportMessageSender extends AbstractMessageSender implements MessageListener<GroupBatch> {
    private final MessageRouter<GroupBatch> router;
    private volatile SubscriberMonitor subscriberMonitor;

    public TransportMessageSender(IServiceProxy serviceProxy,
                                  MessageRouter<GroupBatch> router,
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

        logger.info("Subscribing to transport queue with messages to send");

        subscriberMonitor = router.subscribeAll(this, SEND_ATTRIBUTE);
    }

    public void stop() throws IOException {
        if (subscriberMonitor == null) {
            throw new IllegalStateException("Not yet start subscribe");
        }

        logger.info("Stop listener the 'sender' transport queue");
        try {
            subscriberMonitor.unsubscribe();
        } catch (Exception e) {
            logger.error("Can not unsubscribe", e);
        }
    }

    @Override
    public void handle(DeliveryMetadata deliveryMetadata, GroupBatch message) {
        for (MessageGroup group : message.getGroups()) {
            try {
                List<Message<?>> groupMessages = group.getMessages();
                if (groupMessages.size() != 1) {
                    logger.error("Messages group contain unexpected number of messages {}. Consumer tag: {}",
                            groupMessages.size(), deliveryMetadata.getConsumerTag());
                    continue;
                }
                Message<?> groupMessage = groupMessages.get(0);
                if (groupMessage instanceof RawMessage) {
                    sendMessage(new TransportHolder((RawMessage) groupMessage, message));
                } else {
                    logger.error("Group has an unexpected parsed message with ID: {}. Consumer tag: {}",
                            groupMessage.getId(), deliveryMetadata.getConsumerTag());
                }
            } catch (InterruptedException e) {
                logger.error("Send message operation interrupted. Consumer tag {}", deliveryMetadata.getConsumerTag(), e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Could not send IMessage. Consumer tag {}", deliveryMetadata.getConsumerTag(), e);
            }
        }
    }

    @Override
    public void onClose() {
        logger.info("Closing transport messages sender");
    }

    private static class TransportHolder implements MessageHolder {
        private final RawMessage transportMsg;
        private final GroupBatch batch;

        private TransportHolder(RawMessage transportMsg, GroupBatch batch) {
            this.transportMsg = requireNonNull(transportMsg, "transport raw message");
            this.batch = requireNonNull(batch, "transport batch");
        }

        @Nullable
        @Override
        public EventID getParentEventId() {
            EventId eventId = transportMsg.getEventId();
            return eventId == null ? null : EventUtilsKt.toProto(eventId);
        }

        @Override
        public MessageID getId() {
            return MessageUtilsKt.toProto(transportMsg.getId(), batch);
        }

        @Override
        public Map<String, String> getProperties() {
            return transportMsg.getMetadata();
        }

        @Override
        public byte[] getBody() {
            ByteBuf body = transportMsg.getBody();
            int readerIndex = body.readerIndex();
            try {
                byte[] bodyBytes = new byte[body.readableBytes()];
                body.readBytes(bodyBytes);
                return bodyBytes;
            } finally {
                body.readerIndex(readerIndex);
            }
        }
    }

}
