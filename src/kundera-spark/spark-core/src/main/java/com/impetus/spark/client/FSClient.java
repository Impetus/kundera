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
import org.apache.spark.sql.hive.HiveContext;

import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.spark.client.FileFormatConstants.FileType;
import com.impetus.spark.constants.SparkPropertiesConstants;

/**
 * The Class FSClient.
 * 
 * @author: Devender Yadav
 */
public class FSClient extends FilePathBuilder implements SparkDataClient
{

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
        String dataSourcePath = getInputFilePath(sparkClient.properties);
        String ext = ((String) sparkClient.properties.get("format")).toLowerCase();
        FileType fileType = FileFormatConstants.extension.get(ext);
        switch (fileType)
        {
        case CSV:
            registerTableForCsv(m.getTableName(), dataSourcePath, sparkClient.sqlContext);
            break;
        case JSON:
            registerTableForJson(m.getTableName(), dataSourcePath, sparkClient.sqlContext);
            break;
        default:
            throw new UnsupportedOperationException("Files of type " + ext + " are not yet supported.");
        }
    }

    /**
     * Register table for json.
     * 
     * @param tableName
     *            the table name
     * @param dataSourcePath
     *            the data source path
     * @param sqlContext
     *            the sql context
     */
    private void registerTableForJson(String tableName, String dataSourcePath, HiveContext sqlContext)
    {
        sqlContext.jsonFile(dataSourcePath).registerTempTable(tableName);
    }

    /**
     * Register table for csv.
     * 
     * @param tableName
     *            the table name
     * @param dataSourcePath
     *            the data source path
     * @param sqlContext
     *            the sql context
     */
    private void registerTableForCsv(String tableName, String dataSourcePath, HiveContext sqlContext)
    {
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("header", "true");
        options.put("path", dataSourcePath);
        sqlContext.load(SparkPropertiesConstants.SOURCE_CSV, options).registerTempTable(tableName);
    }

    /**
     * Gets the input file path.
     * 
     * @param properties
     *            the properties
     * @return the input file path
     */
    @Override
    public String getInputFilePath(Map<String, Object> properties)
    {
        String path = (String) properties.get(SparkPropertiesConstants.FS_INPUT_FILE_PATH);
        if (path == null || path.isEmpty())
        {
            throw new KunderaException(
                    "Please set the path of inputfile while creating EntityManager using the property" + "\""
                            + SparkPropertiesConstants.FS_INPUT_FILE_PATH + "\".");
        }
        return path;
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
        Seq s = scala.collection.JavaConversions.asScalaBuffer(listEntity).toList();
        ClassTag tag = scala.reflect.ClassTag$.MODULE$.apply(m.getEntityClazz());
        JavaRDD personRDD = sparkClient.sparkContext.parallelize(s, 1, tag).toJavaRDD();
        DataFrame df = sparkClient.sqlContext.createDataFrame(personRDD, m.getEntityClazz());

        String outputFilePath = getOutputFilePath(sparkClient.properties);
        String ext = (String) sparkClient.properties.get("format");
        FileType fileType = FileFormatConstants.extension.get(ext);
        switch (fileType)
        {
        case CSV:
            return writeDataInCsvFile(df, outputFilePath);
        case JSON:
            return writeDataInJsonFile(df, outputFilePath);
        default:
            throw new UnsupportedOperationException("Files of type " + ext + " are not yet supported.");
        }
    }

    /**
     * Write data in csv file.
     * 
     * @param df
     *            the df
     * @param outputFilePath
     *            the output file path
     * @return true, if successful
     */
    private boolean writeDataInCsvFile(DataFrame df, String outputFilePath)
    {
        // TODO change savemode to APPEND or ErrorIfExists as supported by
        // latest version
        df.save(outputFilePath, SparkPropertiesConstants.SOURCE_CSV, SaveMode.Overwrite);
        return true;
    }

    /**
     * Write data in json file.
     * 
     * @param df
     *            the df
     * @param m
     *            the m
     * @param outputFilePath
     *            the output file path
     * @param sqlContext
     *            the sql context
     * @return true, if successful
     */
    private boolean writeDataInJsonFile(DataFrame df, String outputFilePath)
    {
        // TODO change savemode to APPEND or ErrorIfExists as supported by
        // latest version
        df.save(outputFilePath, "json", SaveMode.Overwrite);
        return true;
    }

    /**
     * Gets the output file path.
     * 
     * @param properties
     *            the properties
     * @return the output file path
     */
    @Override
    public String getOutputFilePath(Map<String, Object> properties)
    {
        String path = (String) properties.get(SparkPropertiesConstants.FS_OUTPUT_FILE_PATH);
        if (path == null || path.isEmpty())
        {
            throw new KunderaException(
                    "Please set the path of outputfile while creating EntityManager using the property" + "\""
                            + SparkPropertiesConstants.FS_OUTPUT_FILE_PATH + "\".");
        }
        return path;
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

        FileType fileType = FileFormatConstants.extension.get(((String) properties.get("format")).toLowerCase());
        switch (fileType)
        {
        case CSV:
            writeDataInCsvFile(dataFrame, getOutputFilePath(properties));
            break;
        case JSON:
            writeDataInJsonFile(dataFrame, getOutputFilePath(properties));
            break;
        default:
            throw new UnsupportedOperationException("Files of type " + properties.get("format")
                    + " are not yet supported.");
        }

    }

}
