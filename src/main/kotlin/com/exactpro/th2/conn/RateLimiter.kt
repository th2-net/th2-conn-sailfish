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

import java.lang.Double.min
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.locks.LockSupport
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class RateLimiter(rate: Int) {
    init {
        require(rate > 0) { "rate must be positive" }
    }

    private val maxPermits = rate.toDouble()
    private val permitDuration = SECONDS.toNanos(1) / maxPermits
    private var freePermits = 0.0
    private var syncTime = 0L

    fun acquire() = acquire(1)

    fun acquire(permits: Int) {
        var currentTime = System.nanoTime()
        val waitUntilTime = getWaitUntilTime(permits, currentTime)

        while (waitUntilTime > currentTime) {
            LockSupport.parkNanos(waitUntilTime - currentTime)
            currentTime = System.nanoTime()
        }
    }

    private fun getWaitUntilTime(permits: Int, currentTime: Long): Long = synchronized(this) {
        if (currentTime > syncTime) {
            val newPermits = (currentTime - syncTime) / permitDuration
            freePermits = min(maxPermits, freePermits + newPermits)
            syncTime = currentTime
        }

        val waitUntilTime = syncTime
        val stalePermits = min(permits.toDouble(), freePermits)
        val freshPermits = permits - stalePermits
        val syncTimeOffset = (freshPermits * permitDuration).toLong()

        syncTime += syncTimeOffset
        freePermits -= stalePermits

        return waitUntilTime
    }
}