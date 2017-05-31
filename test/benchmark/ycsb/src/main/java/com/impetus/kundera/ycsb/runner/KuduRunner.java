/**
 * Copyright 2017 Impetus Infotech.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.utils.MailUtils;
import common.Logger;

/**
 * @author karthikp.manchala
 * 
 */
public class KuduRunner extends YCSBRunner
{
    private String url;

    private static Logger logger = Logger.getLogger(KuduRunner.class);

    public KuduRunner(final String propertyFile, final Configuration config)
    {
        super(propertyFile, config);
        // this.startMongoServerCommand = config.getString("server.location");
        // operationUtils = new MongoDBOperationUtils();
        url = "mongodb://" + host + ":" + port;
    }

    @Override
    public void startServer(boolean performCleanUp, Runtime runTime)
    {
    }

    @Override
    public void stopServer(Runtime runTime)
    {
    }

    protected void sendMail()
    {
        Map<String, Double> delta = new HashMap<String, Double>();

        double kunderaMongoToNativeDelta = ((timeTakenByClient.get(clients[1]).doubleValue() - timeTakenByClient.get(
                clients[0]).doubleValue())
                / timeTakenByClient.get(clients[1]).doubleValue() * 100);
        delta.put("kunderaMongoToNativeDelta", kunderaMongoToNativeDelta);

        if (kunderaMongoToNativeDelta > 8.0)
        {
            MailUtils.sendMail(delta, isUpdate ? "update" : runType, "mongoDb");
        }
        else
        {
            MailUtils.sendPositiveEmail(delta, isUpdate ? "update" : runType, "mongoDb");

        }

    }
}
