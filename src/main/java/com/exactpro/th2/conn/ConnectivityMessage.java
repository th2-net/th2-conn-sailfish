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

import static com.exactpro.sf.common.messages.MetadataExtensions.getMessageProperties;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.IMetadata;
import com.exactpro.sf.common.messages.MetadataExtensions;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessage.Builder;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.conn.utility.MetadataProperty;
import com.exactpro.th2.conn.utility.SailfishMetadataExtensions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

public class ConnectivityMessage {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityMessage.class);
    public static final long MILLISECONDS_IN_SECOND = 1_000L;
    public static final long NANOSECONDS_IN_MILLISECOND = 1_000_000L;

    private final List<IMessage> sailfishMessages;

    // This variables can be calculated in methods
    private final MessageID messageID;
    private final Timestamp timestamp;

    public ConnectivityMessage(List<IMessage> sailfishMessages, String sessionAlias, Direction direction, long sequence, String sessionGroup) {
        this.sailfishMessages = Collections.unmodifiableList(requireNonNull(sailfishMessages, "Message can't be null"));
        if (sailfishMessages.isEmpty()) {
            throw new IllegalArgumentException("At least one sailfish messages must be passed. Session alias: " + sessionAlias + "; Direction: " + direction);
        }
        messageID = createMessageID(createConnectionID(requireNonNull(sessionAlias, "Session alias can't be null"), sessionGroup),
                requireNonNull(direction, "Direction can't be null"), sequence);
		long time = System.currentTimeMillis();
		LOGGER.warn("Sailfish transforms th2 message to real send message too slow, delay is about {} milliseconds. Please add more hardware resources",
				time - sailfishMessages.get(0).getMetaData().getMsgTimestamp().getTime());
        timestamp = createTimestamp(time);
    }

    public String getSessionAlias() {
        return messageID.getConnectionId().getSessionAlias();
    }

    public RawMessage convertToProtoRawMessage() {
        Builder builder = RawMessage.newBuilder();

        RawMessageMetadata.Builder rawMessageMetadata = createRawMessageMetadataBuilder(messageID, timestamp);
        int totalSize = calculateTotalBodySize(sailfishMessages);
        if (totalSize == 0) {
            throw new IllegalStateException("All messages has empty body: " + sailfishMessages);
        }

        byte[] bodyData = new byte[totalSize];
        int index = 0;
        for (IMessage message : sailfishMessages) {
            IMetadata sfMetadata = message.getMetaData();
            if (SailfishMetadataExtensions.contains(sfMetadata, MetadataProperty.PARENT_EVENT_ID)) {
                EventID parentEventID = SailfishMetadataExtensions.getParentEventID(sfMetadata);
                // Should never happen because the Sailfish does not support sending multiple messages at once
                if (builder.hasParentEventId()) {
                    LOGGER.warn("The parent ID is already set for message {}. Current ID: {}, New ID: {}", messageID, builder.getParentEventId(), parentEventID);
                }
                builder.setParentEventId(parentEventID);
            }
            rawMessageMetadata.putAllProperties(defaultIfNull(getMessageProperties(sfMetadata), Collections.emptyMap()));

            byte[] rawMessage = MetadataExtensions.getRawMessage(sfMetadata);
            if (rawMessage == null) {
                LOGGER.warn("The message has empty raw data {}: {}", message.getName(), message);
                continue;
            }
            System.arraycopy(rawMessage, 0, bodyData, index, rawMessage.length);
            index += rawMessage.length;
        }

        return builder.setMetadata(rawMessageMetadata)
                        .setBody(ByteString.copyFrom(bodyData))
                        .build();
    }

    public MessageID getMessageID() {
        return messageID;
    }

    public long getSequence() {
        return messageID.getSequence();
    }

    public Direction getDirection() {
        return messageID.getDirection();
    }

    public List<IMessage> getSailfishMessages() {
        return sailfishMessages;
    }

	@Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("messageID", shortDebugString(messageID))
                .append("timestamp", shortDebugString(timestamp))
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

    private static ConnectionID createConnectionID(String sessionAlias, String sessionGroup) {
		ConnectionID.Builder builder = ConnectionID.newBuilder()
				.setSessionAlias(sessionAlias);
		if (sessionGroup != null) {
			builder.setSessionGroup(sessionGroup);
		}
        return builder.build();
    }

    private static MessageID createMessageID(ConnectionID connectionId, Direction direction, long sequence) {
        return MessageID.newBuilder()
                .setConnectionId(connectionId)
                .setDirection(direction)
                .setSequence(sequence)
                .build();
    }

    private static RawMessageMetadata.Builder createRawMessageMetadataBuilder(MessageID messageID, Timestamp timestamp) {
        return RawMessageMetadata.newBuilder()
                .setId(messageID)
                .setTimestamp(timestamp);
    }

    // TODO: Required nanosecond accuracy
    private static Timestamp createTimestamp(long milliseconds) {
        return Timestamp.newBuilder()
                .setSeconds(milliseconds / MILLISECONDS_IN_SECOND)
                .setNanos((int) (milliseconds % MILLISECONDS_IN_SECOND * NANOSECONDS_IN_MILLISECOND))
                .build();
    }
}
