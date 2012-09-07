/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.cassandra.common;

/**
 * Holds constants for kundera-cassandra module
 * 
 * @author amresh.singh
 */
public interface CassandraConstants
{
    public static final String CQL_VERSION_2_0 = "2.0.0";

    public static final String CQL_VERSION_3_0 = "3.0.0";

    // properties to set during creation of keyspace.
    public final static String PLACEMENT_STRATEGY = "placement_strategy";

    public final static String REPLICATION_FACTOR = "replication_factor";

    public final static String CF_DEFS = "cf_defs";

    public final static String DATA_CENTERS = "datacenters";

    public final static String INVERTED_INDEXING_ENABLED = "inverted.indexing.enabled";

}
