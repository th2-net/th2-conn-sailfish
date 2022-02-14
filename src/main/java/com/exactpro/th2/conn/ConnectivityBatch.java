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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.RawMessageBatch;

import static com.exactpro.th2.conn.utility.ConnectivityMessageExtensionsKt.checkAliasAndDirection;
import static com.exactpro.th2.conn.utility.ConnectivityMessageExtensionsKt.checkForUnorderedSequences;

public class ConnectivityBatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityBatch.class);

    private final String sessionAlias;
    private final long sequence;
    private final Direction direction;
    private final List<ConnectivityMessage> connectivityMessages;

    public ConnectivityBatch(List<ConnectivityMessage> connectivityMessages) {
        if (Objects.requireNonNull(connectivityMessages, "Message list can't be null").isEmpty()) {
            throw new IllegalArgumentException("Message list can't be empty");
        }

        ConnectivityMessage firstMessage = connectivityMessages.get(0);
        this.sessionAlias = firstMessage.getSessionAlias();
        this.direction = firstMessage.getDirection();

        checkAliasAndDirection(connectivityMessages, sessionAlias, direction);

        if (LOGGER.isErrorEnabled()) {
            try {
                checkForUnorderedSequences(connectivityMessages);
            } catch (IllegalStateException e) {
                LOGGER.error("Element`s sequence validation error for session alias '{}' and direction '{}': {}", sessionAlias, direction, e.getMessage());
            }
            // FIXME: Replace logging to thowing exception after solving message reordering problem
            //            throw new IllegalArgumentException("List " + iMessages.stream()
            //                    .map(ConnectivityMessage::getSequence)
            //                    .collect(Collectors.toList())+ " hasn't elements with incremental sequence with one step");
        }
        this.connectivityMessages = List.copyOf(connectivityMessages);
        this.sequence = firstMessage.getSequence();
    }

    public RawMessageBatch convertToProtoRawBatch() {
        return RawMessageBatch.newBuilder()
                .addAllMessages(connectivityMessages.stream()
                        .map(ConnectivityMessage::convertToProtoRawMessage)
                        .collect(Collectors.toList()))
                .build();
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public long getSequence() {
        return sequence;
    }

    public Direction getDirection() {
        return direction;
    }

    public List<ConnectivityMessage> getMessages() {
        return connectivityMessages;
    }

}
