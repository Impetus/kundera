/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.spark.client;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * A factory for creating DataClient objects.
 * 
 * @author: karthikp.manchala
 */
public class SparkDataClientFactory
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(SparkDataClientFactory.class);

    /** The client pool. */
    private static Map<String, SparkDataClient> clientPool = new HashMap<String, SparkDataClient>();

    /** The client name to class. */
    private static Map<String, String> clientNameToClass = new HashMap<String, String>();
    static
    {
        clientNameToClass.put("teradata", "com.impetus.spark.client.TeradataSparkClient");
        clientNameToClass.put("cassandra", "com.impetus.spark.client.CassSparkClient");
        clientNameToClass.put("mongodb", "com.impetus.spark.client.MongoSparkClient");
        clientNameToClass.put("hdfs", "com.impetus.spark.client.HDFSClient");
        clientNameToClass.put("fs", "com.impetus.spark.client.FSClient");
        clientNameToClass.put("hive", "com.impetus.spark.client.HiveSparkClient");
    }

    /**
     * Gets the data client.
     * 
     * @param clientName
     *            the client name
     * @return the data client
     */
    public static SparkDataClient getDataClient(String clientName)
    {

        if (clientPool.get(clientName) != null)
        {
            return clientPool.get(clientName);
        }
        try
        {
            SparkDataClient dataClient = (SparkDataClient) KunderaCoreUtils.createNewInstance(Class
                    .forName(clientNameToClass.get(clientName)));
            clientPool.put(clientName, dataClient);
            return dataClient;
        }
        catch (Exception e)
        {
            logger.error(clientName
                    + " client is invalid/not supported. Please check kundera.client in persistence properties.");
            throw new KunderaException(clientName
                    + " client is invalid/not supported. Please check kundera.client in persistence properties.");
        }
    }

}
