/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import java.util.Collections;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.IMetadata;
import com.exactpro.sf.common.messages.MetadataExtensions;
import com.exactpro.sf.common.messages.MsgMetaData;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessage.Builder;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.conn.utility.MetadataProperty;
import com.exactpro.th2.conn.utility.SailfishMetadataExtensions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

public class ConnectivityMessage {
    public static final long MILLISECONDS_IN_SECOND = 1_000L;
    public static final long NANOSECONDS_IN_MILLISECOND = 1_000_000L;

    private final IMessage sailfishMessage;

    // This variables can be calculated in methods
    private final MessageID messageID;
    private final Timestamp timestamp;

    public ConnectivityMessage(IMessage sailfishMessage, String sessionAlias, Direction direction, long sequence) {
        this.sailfishMessage = requireNonNull(sailfishMessage, "Message can't be null");
        messageID = createMessageID(createConnectionID(requireNonNull(sessionAlias, "Session alias can't be null")),
                requireNonNull(direction, "Direction can't be null"), sequence);
        timestamp = createTimestamp(sailfishMessage.getMetaData().getMsgTimestamp().getTime());
    }

    public String getSessionAlias() {
        return messageID.getConnectionId().getSessionAlias();
    }

    public RawMessage convertToProtoRawMessage() {
        Builder builder = RawMessage.newBuilder();
        IMetadata sfMetadata = sailfishMessage.getMetaData();
        if (SailfishMetadataExtensions.contains(sfMetadata, MetadataProperty.PARENT_EVENT_ID)) {
            builder.setParentEventId(SailfishMetadataExtensions.getParentEventID(sfMetadata));
        }
        return builder.setMetadata(createRawMessageMetadata(messageID, timestamp, sfMetadata))
                        .setBody(ByteString.copyFrom(MetadataExtensions.getRawMessage(sfMetadata)))
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

    public IMessage getSailfishMessage() {
        return sailfishMessage;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("messageID", shortDebugString(messageID))
                .append("timestamp", shortDebugString(timestamp))
                .toString();
    }

    private static ConnectionID createConnectionID(String sessionAlias) {
        return ConnectionID.newBuilder()
                .setSessionAlias(sessionAlias)
                .build();
    }

    private static MessageID createMessageID(ConnectionID connectionId, Direction direction, long sequence) {
        return MessageID.newBuilder()
                .setConnectionId(connectionId)
                .setDirection(direction)
                .setSequence(sequence)
                .build();
    }

    private static RawMessageMetadata createRawMessageMetadata(MessageID messageID, Timestamp timestamp, IMetadata metadata) {
        return RawMessageMetadata.newBuilder()
                .setId(messageID)
                .setTimestamp(timestamp)
                .putAllProperties(defaultIfNull(getMessageProperties(metadata), Collections.emptyMap()))
                .build();
    }

    // TODO: Required nanosecond accuracy
    private static Timestamp createTimestamp(long milliseconds) {
        return Timestamp.newBuilder()
                .setSeconds(milliseconds / MILLISECONDS_IN_SECOND)
                .setNanos((int) (milliseconds % MILLISECONDS_IN_SECOND * NANOSECONDS_IN_MILLISECOND))
                .build();
    }
}
