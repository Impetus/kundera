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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.DataFrame;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * The Class TeradataSparkClient.
 * 
 * @author amitkumar
 */
public class TeradataSparkClient implements SparkDataClient
{
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.spark.client.SparkDataClient#registerTable(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.spark.client.SparkClient)
     */
    @Override
    public void registerTable(EntityMetadata m, SparkClient sparkClient)
    {
        String conn = getConnectionString(m);

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", conn);
        options.put("dbtable", m.getTableName());

        sparkClient.sqlContext.load("jdbc", options).registerTempTable(m.getTableName());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.spark.client.SparkDataClient#persist(java.util.List,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.spark.client.SparkClient)
     */
    @Override
    public boolean persist(List listEntity, EntityMetadata m, SparkClient sparkClient)
    {
        throw new KunderaException("Entity persistence in teradata is currently not supported. ");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.spark.client.SparkDataClient#saveDataFrame(org.apache.spark
     * .sql.DataFrame, java.lang.Class, java.util.Map)
     */
    @Override
    public void saveDataFrame(DataFrame dataFrame, Class<?> entityClazz, Map<String, Object> properties)
    {
        throw new KunderaException("Dataframe persistence in teradata is currently not supported. ");
    }

    /**
     * Gets the connection string.
     * 
     * @param m
     *            the m
     * @return the connection string
     */
    public String getConnectionString(EntityMetadata m)
    {
        Properties properties = new Properties();

        String fileName = "teradata.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);

        if (inputStream != null)
        {
            try
            {
                properties.load(inputStream);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Property file: " + fileName + "not found in the classpath", e);
            }
        }
        else
        {
            throw new RuntimeException("Property file: " + fileName + "not found in the classpath");
        }

        String connectionString = "jdbc:teradata://" + properties.getProperty("teradata.host") + "/database="
                + m.getSchema() + ",tmode=ANSI,charset=UTF8,user=" + properties.getProperty("teradata.user")
                + ",password=" + properties.getProperty("teradata.password") + "";
        return connectionString;
    }
}
