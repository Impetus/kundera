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
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.util.JSON;

/**
 * The Class MongoSparkClient.
 */
public class MongoSparkClient implements SparkDataClient, Serializable
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.spark.client.DataClient#registerTable(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.spark.client.SparkClient)
     */
    public void registerTable(EntityMetadata m, SparkClient sparkClient)
    {

        final Class clazz = m.getEntityClazz();
        SparkContext sc = sparkClient.sparkContext;
        Configuration config = new Configuration();
        config.set(
                "mongo.input.uri",
                buildMongoURIPath(sc.getConf().get("hostname"), sc.getConf().get("portname"), m.getSchema(),
                        m.getTableName()));

        JavaRDD<Tuple2<Object, BSONObject>> mongoJavaRDD = sc.newAPIHadoopRDD(config, MongoInputFormat.class,
                Object.class, BSONObject.class).toJavaRDD();

        JavaRDD<Object> mongoRDD = mongoJavaRDD.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, Object>()
        {
            @Override
            public Iterable<Object> call(Tuple2<Object, BSONObject> arg)
            {
                BSONObject obj = arg._2();
                Object javaObject = generateJavaObjectFromBSON(obj, clazz);
                return Arrays.asList(javaObject);
            }
        });

        sparkClient.sqlContext.createDataFrame(mongoRDD, m.getEntityClazz()).registerTempTable(m.getTableName());
    }

    /**
     * Builds the mongo uri path.
     * 
     * @param host
     *            the host
     * @param port
     *            the port
     * @param db
     *            the db
     * @param table
     *            the table
     * @return the string
     */
    protected String buildMongoURIPath(String host, String port, String db, String table)
    {
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append("mongodb://");
        pathBuilder.append(host);
        pathBuilder.append(":");
        pathBuilder.append(port);
        pathBuilder.append("/");
        pathBuilder.append(db);
        pathBuilder.append(".");
        pathBuilder.append(table);
        return pathBuilder.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.spark.client.DataClient#persist(java.util.List,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.spark.client.SparkClient)
     */
    public boolean persist(List listEntity, final EntityMetadata m, final SparkClient sparkClient)
    {
        try
        {
            Seq s = scala.collection.JavaConversions.asScalaBuffer(listEntity).toList();
            ClassTag tag = scala.reflect.ClassTag$.MODULE$.apply(m.getEntityClazz());
            JavaRDD javaRDD = sparkClient.sparkContext.parallelize(s, 1, tag).toJavaRDD();

            final Object key = PropertyAccessorHelper.getId(listEntity.get(0), m);

            JavaPairRDD<Object, BSONObject> javaRDDPair = javaRDD
                    .mapToPair(new PairFunction<Object, Object, BSONObject>()
                    {
                        @Override
                        public Tuple2<Object, BSONObject> call(Object p)
                        {
                            BSONObject bson = new BasicBSONObject();
                            bson = generateBSONFromJavaObject(p);
                            return new Tuple2<Object, BSONObject>(key, bson);
                        }
                    });
            SparkContext sc = sparkClient.sparkContext;
            Configuration outputConfig = new Configuration();
            outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
            outputConfig.set(
                    "mongo.output.uri",
                    buildMongoURIPath(sc.getConf().get("hostname"), sc.getConf().get("portname"), m.getSchema(),
                            m.getTableName()));

            javaRDDPair.saveAsNewAPIHadoopFile("file://dummy", Object.class, BSONObject.class, MongoOutputFormat.class,
                    outputConfig);
            return true;
        }
        catch (Exception e)
        {
            throw new KunderaException("can't persist object(s)", e);
        }

    }

    /**
     * Generate bson from java object.
     * 
     * @param obj
     *            the obj
     * @return the BSON object
     */
    protected BSONObject generateBSONFromJavaObject(Object obj)
    {
        ObjectWriter ow = new ObjectMapper().writer();
        String json = null;
        try
        {
            json = ow.writeValueAsString(obj);
            return (BSONObject) JSON.parse(json);
        }
        catch (JsonGenerationException | JsonMappingException e)
        {
            throw new KunderaException(
                    "Error in converting BSON Object from Java Object due to error in JSON generation/mapping. Caused BY:",
                    e);
        }

        catch (IOException e)
        {
            throw new KunderaException("Error in converting BSON Object from Java Object. Caused BY:", e);
        }

    }

    /**
     * Generate java object from bson.
     * 
     * @param bsonObj
     *            the bsonObj
     * @param clazz
     *            the clazz
     * @return the object
     */
    protected Object generateJavaObjectFromBSON(BSONObject bsonObj, Class clazz)
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JsonParser jsonParser;
        try
        {
            jsonParser = new JsonFactory().createJsonParser(bsonObj.toString());
            return mapper.readValue(jsonParser, clazz);
        }
        catch (JsonParseException e)
        {
            throw new KunderaException(
                    "Error in converting Java Object from BSON Object due to error in JSON parsing. Caused BY:", e);
        }
        catch (IOException e)
        {
            throw new KunderaException("Error in converting Java Object from BSON Object. Caused BY:", e);
        }
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
        throw new KunderaException("Saving data of DataFrame back to MongoDB is currently not supported. ");
    }

}
