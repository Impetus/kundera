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

import java.util.concurrent.ScheduledExecutorService;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.service.Host;

public class RetryServiceTest
{

    @Test
    public void test()
    {
        TestRetryService retryService = new TestRetryService(10);
        
        Assert.assertNotNull(retryService.getExecutorService());
    }

    
    class TestRetryService extends RetryService
    {
        public TestRetryService(int retryDelay)
        {
            super(retryDelay);
        }

        @Override
        protected boolean verifyConnection(Host host)
        {
            return false;
        }

        public ScheduledExecutorService getExecutorService()
        {
            return executor;
        }
        
        @Override
        public void shutdown()
        {
            // Do nothing.
            
        }
    }
}
