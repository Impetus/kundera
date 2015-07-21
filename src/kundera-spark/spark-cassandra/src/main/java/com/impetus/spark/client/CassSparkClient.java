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
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.spark.constants.SparkPropertiesConstants;

/**
 * The Class CassSparkClient.
 * 
 * @author: karthikp.manchala
 */
public class CassSparkClient implements SparkDataClient
{

    /** The Constant KEYSPACE. */
    private static final String KEYSPACE = "keyspace";

    /** The Constant TABLE. */
    private static final String TABLE = "table";

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.spark.client.DataClient#registerTable(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.spark.client.SparkClient)
     */
    @Override
    public void registerTable(EntityMetadata m, SparkClient sparkClient)
    {
        SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(sparkClient.sparkContext);
        Class clazz = m.getEntityClazz();
        JavaRDD cassandraRowsRDD = functions.cassandraTable(m.getSchema(), m.getTableName(),
                CassandraJavaUtil.mapRowTo(clazz));
        sparkClient.sqlContext.createDataFrame(cassandraRowsRDD, clazz).registerTempTable(m.getTableName());

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.spark.client.DataClient#saveDataFrame(org.apache.spark.sql
     * .DataFrame, java.lang.Class, java.util.Map)
     */
    @Override
    public void saveDataFrame(DataFrame dataFrame, Class<?> entityClazz, Map<String, Object> properties)
    {
        Map<String, String> options = new HashMap();
        options.put("c_table", (String) properties.get(TABLE));
        options.put(KEYSPACE, (String) properties.get(KEYSPACE));

        // TODO update order
        dataFrame.save(SparkPropertiesConstants.SOURCE_CASSANDRA,
                SaveMode.Append, options);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.spark.client.DataClient#persist(java.util.List,
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

            CassandraJavaUtil.javaFunctions(personRDD)
                    .writerBuilder(m.getSchema(), m.getTableName(), CassandraJavaUtil.mapToRow(m.getEntityClazz()))
                    .saveToCassandra();
            return true;
        }
        catch (Exception e)
        {
            throw new KunderaException("Cannot persist object(s)", e);
        }

    }

}
