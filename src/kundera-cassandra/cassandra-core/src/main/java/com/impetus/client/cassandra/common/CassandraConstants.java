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
    public static final String PLACEMENT_STRATEGY = "strategy.class";

    public static final String REPLICATION_FACTOR = "replication.factor";

    public static final String CF_DEFS = "cf.defs";

    public static final String DATA_CENTERS = "datacenters";

    public static final String INVERTED_INDEXING_ENABLED = "inverted.indexing.enabled";

    /**
     * 
     */
    public static final String DEFAULT_REPLICATION_FACTOR = "1";

    public static final String DURABLE_WRITES = "durable.writes";
    
    /** The Constant THRIFT BINARY Port. */
    public static final String THRIFT_PORT = "rpc.port";

    // properties to set during creation of column family.

    public static final String DEFAULT_VALIDATION_CLASS = "default.validation.class";

    public static final String KEY_VALIDATION_CLASS = "key.validation.class";

    public static final String COMPARATOR_TYPE = "comparator.type";

    public static final String SUBCOMPARATOR_TYPE = "subcomparator.type";

    public static final String REPLICATE_ON_WRITE = "replicate.on.write";

    public static final String COMPACTION_STRATEGY = "compaction.strategy";

    public static final String MAX_COMPACTION_THRESHOLD = "max.compaction.threshold";

    public static final String MIN_COMPACTION_THRESHOLD = "min.compaction.threshold";

    public static final String COMMENT = "comment";

    public static final String ID = "id";

    public static final String CACHING = "caching";

    public static final String BLOOM_FILTER_FP_CHANCE = "bloom.filter.fp.chance";

    public static final String GC_GRACE_SECONDS = "gc.grace.seconds";

    public static final String READ_REPAIR_CHANCE = "read.repair.chance";

    public static final String DCLOCAL_READ_REPAIR_CHANCE = "dclocal.read.repair.chance";

    public static final String CQL_VERSION = "cql.version";

    /** Name of Row key column when stored using CQL insert statement */
    public static final String CQL_KEY = "key";

    public static final String TEST_ON_BORROW = "testonborrow";

    public static final String TEST_ON_CONNECT = "testonconnect";

    public static final String TEST_WHILE_IDLE = "testwhileidle";

    public static final String TEST_ON_RETURN = "testonretrun";

    public static final String SOCKET_TIMEOUT = "socket.timeout";

    public static final String MAX_WAIT = "max.wait";
}
