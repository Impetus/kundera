/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.impetus.kundera.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Kuldeep.mishra
 * Implements {@link ThreadFactory}. Implementation to handle thread count and return thread for retry on connection attempt.
 */
public class KunderaThreadFactory implements ThreadFactory
{
    private ConcurrentHashMap<String, AtomicInteger> counters = new ConcurrentHashMap<String, AtomicInteger>();

    private final String name;

    public KunderaThreadFactory(final String threadName)
    {
        this.name = threadName;
    }

    private int getNext()
    {
        AtomicInteger threadCount = counters.get(name);
        if (threadCount == null)
        {
            threadCount = new AtomicInteger();
            counters.put(name, threadCount);
        }
        return threadCount.incrementAndGet();
    }

    @Override
    public Thread newThread(Runnable r)
    {
        Thread t = new Thread(r);
        t.setDaemon(true);
        StringBuilder tdNameBuilder = new StringBuilder(name);
        tdNameBuilder.append("#");
        tdNameBuilder.append(getNext());
        t.setName(tdNameBuilder.toString());
        return t;
    }
}
