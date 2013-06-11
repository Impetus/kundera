/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera;

/**
 * Constants.
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public final class Constants
{

    /**
     * Instantiates a new constants.
     */
    private Constants()
    {

    }

    /** The Constant ENCODING. */
    public static final String ENCODING = "utf-8";

    /** UTF-8 character set */
    public static final String CHARSET_UTF8 = "UTF-8";

    /** The Constant SEPARATOR. */
    public final static String FOREIGN_KEY_SEPARATOR = "~";

    /** The Constant INVALID. */
    public final static int INVALID = -1;

    /** The Constant SUPER_COLUMN_NAME_DELIMITER. */
    public final static String EMBEDDED_COLUMN_NAME_DELIMITER = "#";

    /** The Constant TO_ONE_SUPER_COL_NAME. */
    public static final String FOREIGN_KEY_EMBEDDED_COLUMN_NAME = "FKey-TO";

    /** The Constant KUNDERA_SECONDARY_CACHE_NAME. */
    public static final String KUNDERA_SECONDARY_CACHE_NAME = "Kundera";

    /** The Constant PERSISTENCE_UNIT_NAME. */
    public static final String PERSISTENCE_UNIT_NAME = "persistenceUnitName";

    /** The Constant LUCENE_INDEX_DIRECTORY_NAME. */
    public static final String LUCENE_INDEX_DIRECTORY_NAME = "lucene";

    /**
     * Separator used for providing persistence unit alongwith schema on entity
     * class (applicable in case of cross-datastore persistence).
     */
    public static final String SCHEMA_PERSISTENCE_UNIT_SEPARATOR = "@";

    /**
     * Separator used for providing a list of persistence units while creating
     * EMF.
     */
    public final static String PERSISTENCE_UNIT_SEPARATOR = ",";

    /**
     * Name of column family(HBase) or super column(cassandra) which houses all
     * join columns.
     */
    public final static String JOIN_COLUMNS_FAMILY_NAME = "JoinColumns";

    public final static String JOIN_COLUMN_NAME_SEPARATOR = "_";

    public final static String NODE_ID_SEPARATOR = "$";

    public final static String RDBMS_CLIENT_FACTORY = "com.impetus.client.rdbms.RDBMSClientFactory";

    public final static String NEO4J_CLIENT_FACTORY = "com.impetus.client.neo4j.Neo4JClientFactory";

    public final static int DEFAULT_MAX_FETCH_DEPTH = 2;

    public final static String INDEX_TABLE_SUFFIX = "_INVRTD_IDX";

    public final static String INDEX_TABLE_ROW_KEY_DELIMITER = ".";

    public final static String INDEX_TABLE_EC_DELIMITER = "@SuperColumn:";

    public final static String LOADBALANCING_POLICY = "loadbalancing.policy";
    
    public final static String FAILOVER_POLICY = "failover.policy";

    public final static String FAIL_FAST = "fail.fast";

    public final static String ON_FAIL_TRY_ONE_NEXT_AVAILABLE = "on.fail.try.one.next.available";

    public final static String ON_FAIL_TRY_ALL_AVAILABLE = "on.fail.try.all.available";
    
    public final static String RETRY_DELAY = "retry.delay";
    
    public final static String RETRY = "retry";

    // public final static String INVERTED_INDEXING_ENABLED =
    // "inverted.indexing.enabled";

    // public final static String ZOOKEEPER_PORT = "zookeeper_port";
    //
    // public final static String ZOOKEEPER_HOST = "zookeeper_host";

    // public final static String PLACEMENT_STRATEGY = "placement_strategy";
    //
    // public final static String REPLICATION_FACTOR = "replication_factor";
    //
    // public final static String CF_DEFS = "cf_defs";
    //
    // public final static String DATA_CENTERS = "datacenters";
    //
    // public final static String CONNECTIONS = "mongodb.servers";
    //
    // public final static String SOCKET_TIMEOUT = "socket.timeout";
    //
    // public final static String READ_PREFERENCE = "read.preference";
}
