package com.impetus.kundera.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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
        if (threadCount==null)
        {
            counters.put(name, new AtomicInteger());
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
