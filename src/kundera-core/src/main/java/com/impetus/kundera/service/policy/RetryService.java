/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.service.policy;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import com.impetus.kundera.service.Host;
import com.impetus.kundera.utils.KunderaThreadFactory;

public abstract class RetryService
{

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
