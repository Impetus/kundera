/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.ycsb.runner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.utils.MailUtils;
import com.impetus.kundera.ycsb.utils.RedisOperationUtils;
import common.Logger;

/**
 * @author vivek mishra
 * 
 */
public class RedisRunner extends YCSBRunner
{
    private static RedisOperationUtils operationUtils;

    private String redisServerLocation;

    private static Logger logger = Logger.getLogger(RedisRunner.class);

    public RedisRunner(final String propertyFile, final Configuration config)
    {
        super(propertyFile, config);
        this.redisServerLocation = config.getString("server.location");
        operationUtils = new RedisOperationUtils();
    }

    @Override
    public void startServer(boolean performCleanup, Runtime runTime)
    {
        try
        {
            operationUtils.startRedisServer(runTime, redisServerLocation);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void stopServer(Runtime runTime)
    {
        try
        {
            operationUtils.stopRedisServer(runTime);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    protected void sendMail()
    {
        Map<String, Double> delta = new HashMap<String, Double>();

        double kunderaRedisToJedisDelta = ((timeTakenByClient.get(clients[1]).doubleValue() - timeTakenByClient.get(
                clients[0]).doubleValue())
                / timeTakenByClient.get(clients[1]).doubleValue() * 100);
        delta.put("kunderaRedisToJedisDelta", kunderaRedisToJedisDelta);

        if (kunderaRedisToJedisDelta > 10.00)
        {
            MailUtils.sendMail(delta, isUpdate ? "update" : runType, "redis");
        }
        else
        {
            MailUtils.sendPositiveEmail(delta, isUpdate ? "update" : runType, "redis");
        }

    }
}
