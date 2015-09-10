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
package com.impetus.spark.constants;

/**
 * The Class PropertiesConstants.
 * 
 * @author: karthikp.manchala
 */
public final class SparkPropertiesConstants
{
    /** The Constant FS_OUTPUT_FILE_PATH. */
    public static final String FS_OUTPUT_FILE_PATH = "kundera.fs.outputfile.path";

    /** The Constant FS_INPUT_FILE_PATH. */
    public static final String FS_INPUT_FILE_PATH = "kundera.fs.inputfile.path";

    /** The Constant HDFS_OUTPUT_FILE_PATH. */
    public static final String HDFS_OUTPUT_FILE_PATH = "kundera.hdfs.outputfile.path";

    /** The Constant HDFS_INPUT_FILE_PATH. */
    public static final String HDFS_INPUT_FILE_PATH = "kundera.hdfs.inputfile.path";

    /** The Constant HDFS_CONNECTION_PORT. */
    public static final String HDFS_CONNECTION_PORT = "kundera.hdfs.connection.port";

    /** The Constant HDFS_CONNECTION_HOST. */
    public static final String HDFS_CONNECTION_HOST = "kundera.hdfs.connection.host";

    /** The Constant SOURCE_CSV. */
    public static final String SOURCE_CSV = "com.databricks.spark.csv";

    /** The Constant SOURCE_CASSANDRA. */
    public static final String SOURCE_CASSANDRA = "org.apache.spark.sql.cassandra";

    /** The Constant CLIENT_CASSANDRA. */
    public static final String CLIENT_CASSANDRA = "cassandra";

    /** The Constant CLIENT_FS. */
    public static final String CLIENT_FS = "fs";

    /** The Constant CLIENT_HDFS. */
    public static final String CLIENT_HDFS = "hdfs";

    /** The Constant CLIENT_MONGODB. */
    public static final String CLIENT_MONGODB = "mongodb";

    /** The Constant CLIENT_HIVE. */
    public static final String CLIENT_HIVE = "hive";
}
