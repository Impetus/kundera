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
public interface Constants
{

    /** The Constant ENCODING. */
    public static final String ENCODING = "utf-8";

    /** UTF-8 character set. */
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

    /** The Constant JOIN_COLUMN_NAME_SEPARATOR. */
    public final static String JOIN_COLUMN_NAME_SEPARATOR = "_";

    /** The Constant NODE_ID_SEPARATOR. */
    public final static String NODE_ID_SEPARATOR = "$";

    /** The Constant RDBMS_CLIENT_FACTORY. */
    public final static String RDBMS_CLIENT_FACTORY = "com.impetus.client.rdbms.RDBMSClientFactory";

    /** The Constant NEO4J_CLIENT_FACTORY. */
    public final static String NEO4J_CLIENT_FACTORY = "com.impetus.client.neo4j.Neo4JClientFactory";

    /** The Constant REDIS_CLIENT_FACTORY. */
    public final static String REDIS_CLIENT_FACTORY = "com.impetus.client.redis.RedisClientFactory";

    /** The Constant DEFAULT_MAX_FETCH_DEPTH. */
    public final static int DEFAULT_MAX_FETCH_DEPTH = 2;

    /** The Constant INDEX_TABLE_SUFFIX. */
    public final static String INDEX_TABLE_SUFFIX = "_INVRTD_IDX";

    /** The Constant INDEX_TABLE_ROW_KEY_DELIMITER. */
    public final static String INDEX_TABLE_ROW_KEY_DELIMITER = ".";

    /** The Constant INDEX_TABLE_EC_DELIMITER. */
    public final static String INDEX_TABLE_EC_DELIMITER = "@SuperColumn:";

    /** The Constant LOADBALANCING_POLICY. */
    public final static String LOADBALANCING_POLICY = "loadbalancing.policy";

    /** The Constant FAILOVER_POLICY. */
    public final static String FAILOVER_POLICY = "failover.policy";

    /** The Constant FAIL_FAST. */
    public final static String FAIL_FAST = "fail.fast";

    /** The Constant ON_FAIL_TRY_ONE_NEXT_AVAILABLE. */
    public final static String ON_FAIL_TRY_ONE_NEXT_AVAILABLE = "on.fail.try.one.next.available";

    /** The Constant ON_FAIL_TRY_ALL_AVAILABLE. */
    public final static String ON_FAIL_TRY_ALL_AVAILABLE = "on.fail.try.all.available";

    /** The Constant RETRY_DELAY. */
    public final static String RETRY_DELAY = "retry.delay";

    /** The Constant RETRY. */
    public final static String RETRY = "retry";

    /** The Constant PERSISTENCE_UNIT_LOCATIION. */
    public final static String PERSISTENCE_UNIT_LOCATIION = "persistenceunit.location";

    /** The Constant DEFAULT_PERSISTENCE_UNIT_LOCATIION. */
    public final static String DEFAULT_PERSISTENCE_UNIT_LOCATIION = "META-INF/persistence.xml";

    /** The Constant SPACE. */
    public final static String SPACE = " ";

    /** The Constant DEFAULT_TIMESTAMP_GENERATOR. */
    public final static String DEFAULT_TIMESTAMP_GENERATOR = "default.timestamp.generator";

    /** The Constant AGGREGATIONS. */
    public final static String AGGREGATIONS = "aggregations";

    /** The Constant PRIMARY_KEYS. */
    public final static String PRIMARY_KEYS = "primaryKeys";

    /** The Constant SELECT_EXPRESSION_ORDER. */
    public final static String SELECT_EXPRESSION_ORDER = "selectExpressionOrder";

    /** The Constant COL_FAMILY. */
    public static final String COL_FAMILY = "colFamily";

    /** The Constant DB COL_NAME. */
    public static final String DB_COL_NAME = "dbColumn";

    /** The Constant COL_NAME. */
    public static final String COL_NAME = "colName";

    /** The Constant IGNORE_CASE. */
    public static final String IGNORE_CASE = "ignoreCase";

    /** The Constant COMPOSITE. */
    public final static String COMPOSITE = "composite";

    /** The Constant IS_EMBEDDABLE. */
    public static final String IS_EMBEDDABLE = "isEmbeddable";

    /** The Constant FIELD_CLAZZ. */
    public static final String FIELD_CLAZZ = "fieldClazz";

    /** The Constant FIELD_NAME. */
    public static final String FIELD_NAME = "fieldName";

    /** The Constant ESCAPE_QUOTE. */
    public static final String ESCAPE_QUOTE = "\"";

    /** The Constant OPEN_SQUARE_BRACKET. */
    public static final String OPEN_SQUARE_BRACKET = "[";

    /** The Constant CLOSE_SQUARE_BRACKET. */
    public static final String CLOSE_SQUARE_BRACKET = "]";

    /** The Constant OPEN_ROUND_BRACKET. */
    public static final String OPEN_ROUND_BRACKET = "(";

    /** The Constant CLOSE_ROUND_BRACKET. */
    public static final String CLOSE_ROUND_BRACKET = ")";

    /** The Constant OPEN_CURLY_BRACKET. */
    public static final String OPEN_CURLY_BRACKET = "{";

    /** The Constant COMMA. */
    public static final String COMMA = ",";

    /** The Constant CLOSE_CURLY_BRACKET. */
    public static final String CLOSE_CURLY_BRACKET = "}";

    /** The Constant COLON. */
    public static final String COLON = ":";

    /** The Constant SEMI_COLON. */
    public static final String SEMI_COLON = ";";

    /** The Constant STR_GT. */
    public static final String STR_GT = ">";

    /** The Constant STR_LT. */
    public static final String STR_LT = "<";

    /** The Constant SPACE_COMMA. */
    public static final String SPACE_COMMA = " ,";

}
