/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn

import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.locks.LockSupport.parkNanos
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class RateLimiter(rate: Int, refillsPerSecond: Int) {
    constructor(rate: Int) : this(rate, 10)

    private var tokens = 0L
    private val maxTokens = rate / refillsPerSecond
    private var nextRefillTime = 0L
    private val refillInterval = SECONDS.toNanos(1) / refillsPerSecond

    fun acquire() {
        while (!tryAcquire()) parkNanos(1_000)
    }

    fun tryAcquire(): Boolean = synchronized(this) {
        if (tokens++ < maxTokens) return true
        val currentTime = System.nanoTime()
        if (currentTime < nextRefillTime) return false
        nextRefillTime = currentTime + refillInterval
        tokens = 1
        return true
    }
}