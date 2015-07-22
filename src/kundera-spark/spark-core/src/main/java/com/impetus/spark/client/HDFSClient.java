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

import java.util.Map;

import org.apache.spark.sql.DataFrame;

import com.impetus.kundera.KunderaException;
import com.impetus.spark.constants.SparkPropertiesConstants;

/**
 * The Class HDFSClient.
 * 
 * @author: karthikp.manchala
 */
public class HDFSClient extends FSClient
{

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.spark.client.FSClient#getInputFilePath(java.util.Map)
     */
    @Override
    public String getInputFilePath(Map<String, Object> properties)
    {
        String path = (String) properties.get(SparkPropertiesConstants.HDFS_INPUT_FILE_PATH);
        if (path == null || path.isEmpty())
        {
            throw new KunderaException(
                    "Please set the path of inputfile while creating EntityManager using the property" + "\""
                            + SparkPropertiesConstants.HDFS_INPUT_FILE_PATH + "\".");
        }

        return path;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.spark.client.FSClient#getOutputFilePath(java.util.Map)
     */
    @Override
    public String getOutputFilePath(Map<String, Object> properties)
    {
        String path = (String) properties.get(SparkPropertiesConstants.HDFS_OUTPUT_FILE_PATH);
        if (path == null || path.isEmpty())
        {
            throw new KunderaException(
                    "Please set the path of outputfile while creating EntityManager using the property" + "\""
                            + SparkPropertiesConstants.HDFS_OUTPUT_FILE_PATH + "\".");
        }
        return path;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.spark.client.FSClient#saveDataFrame(org.apache.spark.sql.
     * DataFrame, java.lang.Class, java.util.Map)
     */
    @Override
    public void saveDataFrame(DataFrame dataFrame, Class<?> entityClazz, Map<String, Object> properties)
    {
        dataFrame.save(getOutputFilePath(properties), "json");
    }

}
