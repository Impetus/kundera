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
package com.impetus.kundera.ycsb.runner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.utils.MailUtils;
import com.impetus.kundera.ycsb.utils.OracleNosqlOperationUtils;

/**
 * @author Kuldeep mishra
 * 
 */
public class OracleNosqlRunner extends YCSBRunner
{
    private static OracleNosqlOperationUtils operationUtils;

    public OracleNosqlRunner(final String propertyFile, final Configuration config)
    {
        super(propertyFile, config);
        operationUtils = new OracleNosqlOperationUtils();
    }

    @Override
    public void startServer(boolean performCleanup, Runtime runTime)
    {
        try
        {
            operationUtils.cleanAndStartOracleServer(runTime);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void stopServer(Runtime runTime)
    {
        try
        {
            operationUtils.stopOracleServer(runTime);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected void sendMail()
    {
        Map<String, Double> delta = new HashMap<String, Double>();

        double kunderaOracleToNativeDelta = ((timeTakenByClient.get(clients[1]).doubleValue() - timeTakenByClient.get(
                clients[0]).doubleValue())
                / timeTakenByClient.get(clients[1]).doubleValue() * 100);
        delta.put("kunderaOracleToNativeDelta", kunderaOracleToNativeDelta);

        if (kunderaOracleToNativeDelta > 10.00)
        {
            MailUtils.sendMail(delta, isUpdate ? "update" : runType, "oracle");
        }
        else
        {
            MailUtils.sendPositiveEmail(delta, isUpdate ? "update" : runType, "oracle");
        }

    }
}
