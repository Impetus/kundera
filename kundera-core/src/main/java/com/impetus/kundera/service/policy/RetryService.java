package com.impetus.kundera.service.policy;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.service.Host;
import com.impetus.kundera.utils.KunderaThreadFactory;

public abstract class RetryService
{
    /** log for this class. */
    private static Log logger = LogFactory.getLog(RetryService.class);

    protected final ScheduledExecutorService executor;

    protected ScheduledFuture<?> sf;

    protected int retryDelayInSeconds = 100;

    public RetryService(int retryDelay)
    {
        if (retryDelay > 0)
        {
            this.retryDelayInSeconds = retryDelay;
        }
        this.executor = Executors.newScheduledThreadPool(1, new KunderaThreadFactory(RetryService.class.getName()));
    }

    protected abstract boolean verifyConnection(Host host);

    public abstract void shutdown();
}
