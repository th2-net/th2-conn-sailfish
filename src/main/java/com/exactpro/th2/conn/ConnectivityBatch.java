/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.RawMessageBatch;

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
        checkMessages(connectivityMessages, sessionAlias, direction);

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

    private static void checkMessages(List<ConnectivityMessage> iMessages, String sessionAlias, Direction direction) {
        if (iMessages.isEmpty()) {
            throw new IllegalArgumentException("List can't be empty");
        }

        if (!iMessages.stream()
                .allMatch(iMessage -> Objects.equals(sessionAlias, iMessage.getSessionAlias())
                        && direction == iMessage.getDirection())) {
            throw new IllegalArgumentException("List " + iMessages + " has elements with incorrect metadata, expected session alias '"+ sessionAlias +"' direction '" + direction + '\'');
        }

        List<Long> missedSequences = new ArrayList<>();
        for (int index = 0; index < iMessages.size() - 1; index++) {
            for (long sequence = iMessages.get(index).getSequence() + 1; sequence < iMessages.get(index + 1).getSequence(); sequence++) {
                missedSequences.add(sequence);
            }
        }
        if (LOGGER.isErrorEnabled() && !missedSequences.isEmpty()) {
            LOGGER.error(
                    "List {} hasn't elements with incremental sequence with one step for session alias '{}' and direction '{}'. Missed sequences {}",
                    iMessages.stream()
                            .map(ConnectivityMessage::getSequence)
                            .collect(Collectors.toList()),
                    sessionAlias,
                    direction,
                    missedSequences
            );
        }

            // FIXME: Replace logging to thowing exception after solving message reordering problem
//            throw new IllegalArgumentException("List " + iMessages.stream()
//                    .map(ConnectivityMessage::getSequence)
//                    .collect(Collectors.toList())+ " hasn't elements with incremental sequence with one step");
    }
}
