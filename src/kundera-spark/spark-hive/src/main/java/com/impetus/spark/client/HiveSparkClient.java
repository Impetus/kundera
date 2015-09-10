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

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * The Class HiveSparkClient.
 * 
 * @author amitkumar
 */
public class HiveSparkClient implements SparkDataClient
{
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(HiveSparkClient.class);

    /** The Constant KEYSPACE. */
    private static final String KEYSPACE = "keyspace";

    /** The Constant TABLE. */
    private static final String TABLE = "table";

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
        // All the hive tables are already registered with HiveContext
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
        try
        {
            Seq s = scala.collection.JavaConversions.asScalaBuffer(listEntity).toList();
            ClassTag tag = scala.reflect.ClassTag$.MODULE$.apply(m.getEntityClazz());
            JavaRDD personRDD = sparkClient.sparkContext.parallelize(s, 1, tag).toJavaRDD();

            DataFrame df = sparkClient.sqlContext.createDataFrame(personRDD, m.getEntityClazz());
            sparkClient.sqlContext.sql("use " + m.getSchema());
            if (logger.isDebugEnabled())
            {
                logger.info("Below are the registered table with hive context: ");
                sparkClient.sqlContext.sql("show tables").show();
            }
            df.write().insertInto(m.getTableName());

            return true;
        }
        catch (Exception e)
        {
            throw new KunderaException("Cannot persist object(s)", e);
        }

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
        dataFrame.sqlContext().sql("use " + (String) properties.get(KEYSPACE));
        dataFrame.write().insertInto((String) properties.get(TABLE));
    }
}
