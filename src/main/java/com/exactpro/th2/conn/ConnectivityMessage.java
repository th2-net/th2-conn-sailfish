/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.IMetadata;
import com.exactpro.sf.common.messages.MetadataExtensions;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.RawMessageMetadata.Builder;
import com.exactpro.th2.conn.utility.MetadataProperty;
import com.exactpro.th2.conn.utility.SailfishMetadataExtensions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.exactpro.sf.common.messages.MetadataExtensions.getMessageProperties;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

@SuppressWarnings("ClassNamePrefixedWithPackageName")
public class ConnectivityMessage {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityMessage.class);
    public static final long MILLISECONDS_IN_SECOND = 1_000L;
    public static final long NANOSECONDS_IN_MILLISECOND = 1_000_000L;

    private final List<IMessage> sailfishMessages;
    private final MessageID messageId;

    public ConnectivityMessage(List<IMessage> sailfishMessages, MessageID.Builder messageIdBuilder) {
        requireNonNull(sailfishMessages, "Messages can't be null");
        requireNonNull(messageIdBuilder, "Message id builder can't be null");
        if (sailfishMessages.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "At least one sailfish message must be passed. Message id: '%s'",
                    shortDebugString(messageIdBuilder)
            ));
        }
        this.sailfishMessages = Collections.unmodifiableList(requireNonNull(sailfishMessages, "Message can't be null"));
        LOGGER.warn("Sailfish transforms th2 message to real send message too slow, delay is about 10 milliseconds. Please add more hardware resources");
        this.messageId = messageIdBuilder
                .setTimestamp(createTimestamp())
                .build();
    }

    public RawMessage convertToProtoRawMessage() {
        int totalSize = calculateTotalBodySize(sailfishMessages);
        if (totalSize == 0) {
            throw new IllegalStateException("All messages has empty body: " + sailfishMessages);
        }

        RawMessage.Builder messageBuilder = RawMessage.newBuilder();
        Builder metadataBuilder = RawMessageMetadata.newBuilder().setId(messageId);
        byte[] bodyData = new byte[totalSize];
        int index = 0;
        for (IMessage message : sailfishMessages) {
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
                .setBody(ByteString.copyFrom(bodyData))
                .build();
    }

    public MessageID getMessageId() {
        return messageId;
    }

    public String getBookName() {
        return messageId.getBookName();
    }

    public String getSessionAlias() {
        return messageId.getConnectionId().getSessionAlias();
    }

    public Direction getDirection() {
        return messageId.getDirection();
    }

    public long getSequence() {
        return messageId.getSequence();
    }

    public List<IMessage> getSailfishMessages() {
        return sailfishMessages;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("messageId", shortDebugString(messageId))
                .append("sailfishMessages", sailfishMessages.stream().map(IMessage::getName).collect(Collectors.joining(", ")))
                .toString();
    }

    private static int calculateTotalBodySize(Collection<IMessage> messages) {
        return messages.stream()
                .mapToInt(it -> {
                    byte[] rawMessage = MetadataExtensions.getRawMessage(it.getMetaData());
                    return rawMessage == null ? 0 : rawMessage.length;
                }).sum();
    }

    private static Timestamp createTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }
}
