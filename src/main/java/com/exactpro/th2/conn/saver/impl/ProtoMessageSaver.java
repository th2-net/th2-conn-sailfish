/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.conn.saver.impl;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.IMetadata;
import com.exactpro.sf.common.messages.MetadataExtensions;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.conn.ConnectivityBatch;
import com.exactpro.th2.conn.ConnectivityMessage;
import com.exactpro.th2.conn.saver.MessageSaver;
import com.exactpro.th2.conn.utility.MetadataProperty;
import com.exactpro.th2.conn.utility.SailfishMetadataExtensions;
import com.google.protobuf.UnsafeByteOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.exactpro.sf.common.messages.MetadataExtensions.getMessageProperties;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class ProtoMessageSaver implements MessageSaver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoMessageSaver.class);
    private final MessageRouter<MessageGroupBatch> router;

    public ProtoMessageSaver(MessageRouter<MessageGroupBatch> router) {
        this.router = Objects.requireNonNull(router, "router cannot be null");
    }

    @Override
    public void save(ConnectivityBatch batch) throws IOException {
        router.send(convertToProtoBatch(batch));
    }

    private static MessageGroupBatch convertToProtoBatch(ConnectivityBatch batch) {
        return MessageGroupBatch.newBuilder()
                .addAllGroups(batch.getMessages().stream()
                        .map(ProtoMessageSaver::convertToProtoRawMessage)
                        .map(rawMessage -> MessageGroup.newBuilder().addMessages(
                                AnyMessage.newBuilder().setRawMessage(rawMessage)
                        ).build())
                        .collect(Collectors.toList()))
                .build();
    }

    public static RawMessage convertToProtoRawMessage(ConnectivityMessage connectivityMessage) {
        int totalSize = connectivityMessage.calculateTotalBodySize();

        RawMessage.Builder messageBuilder = RawMessage.newBuilder();
        MessageID messageId = connectivityMessage.getMessageId();
        RawMessageMetadata.Builder metadataBuilder = RawMessageMetadata.newBuilder().setId(messageId);
        byte[] bodyData = new byte[totalSize];
        int index = 0;
        for (IMessage message : connectivityMessage.getSailfishMessages()) {
            IMetadata sfMetadata = message.getMetaData();
            if (SailfishMetadataExtensions.contains(sfMetadata, MetadataProperty.PARENT_EVENT_ID)) {
                EventID parentEventID = SailfishMetadataExtensions.getParentEventID(sfMetadata);
                // Should never happen because the Sailfish does not support sending multiple messages at once
                if (messageBuilder.hasParentEventId()) {
                    LOGGER.warn("The parent ID is already set for message {}. Current ID: {}, New ID: {}", messageId, messageBuilder.getParentEventId(), parentEventID);
                }
                messageBuilder.setParentEventId(parentEventID);
            }
            metadataBuilder.putAllProperties(defaultIfNull(getMessageProperties(sfMetadata), Collections.emptyMap()));

            byte[] rawMessage = MetadataExtensions.getRawMessage(sfMetadata);
            if (rawMessage == null) {
                LOGGER.warn("The message has empty raw data {}: {}", message.getName(), message);
                continue;
            }
            System.arraycopy(rawMessage, 0, bodyData, index, rawMessage.length);
            index += rawMessage.length;
        }

        return messageBuilder.setMetadata(metadataBuilder)
                // we can wrap the array because it is not going to leak anywhere else
                .setBody(UnsafeByteOperations.unsafeWrap(bodyData))
                .build();
    }
}
