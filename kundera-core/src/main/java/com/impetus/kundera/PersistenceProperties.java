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
 * Contains all constants properties supported in persistence.xml
 * 
 * @author amresh.singh
 * 
 */
public interface PersistenceProperties
{

    /** The Constant KUNDERA_NODES. */
    public static final String KUNDERA_NODES = "kundera.nodes";

    /** The Constant KUNDERA_PORT. */
    public static final String KUNDERA_PORT = "kundera.port";

    /** The Constant KUNDERA_KEYSPACE. */
    public static final String KUNDERA_KEYSPACE = "kundera.keyspace";

    /** The Constant KUNDERA_DIALECT. */
    public static final String KUNDERA_DIALECT = "kundera.dialect";

    /** The Constant KUNDERA_CLIENT. */
    public static final String KUNDERA_CLIENT = "kundera.client";

    /** The Constant KUNDERA_CACHE_PROVIDER_CLASS. */
    public static final String KUNDERA_CACHE_PROVIDER_CLASS = "kundera.cache.provider.class";

    /** The Constant KUNDERA_CACHE_CONFIG_RESOURCE. */
    public static final String KUNDERA_CACHE_CONFIG_RESOURCE = "kundera.cache.config.resource";

    /** The Constant KUNDERA_FETCH_MAX_DEPTH. */
    public static final String KUNDERA_FETCH_MAX_DEPTH = "kundera.fetch.max.depth";

    /** Connection Pooling related constants. */

    // Cap on the number of object instances managed by the pool per node.
    public static final String KUNDERA_POOL_SIZE_MAX_ACTIVE = "kundera.pool.size.max.active";

    // Cap on the number of "idle" instances in the pool.
    /** The Constant KUNDERA_POOL_SIZE_MAX_IDLE. */
    public static final String KUNDERA_POOL_SIZE_MAX_IDLE = "kundera.pool.size.max.idle";

    // Minimum number of idle objects to maintain in each of the nodes.
    /** The Constant KUNDERA_POOL_SIZE_MIN_IDLE. */
    public static final String KUNDERA_POOL_SIZE_MIN_IDLE = "kundera.pool.size.min.idle";

    // Cap on the total number of instances from all nodes combined.
    /** The Constant KUNDERA_POOL_SIZE_MAX_TOTAL. */
    public static final String KUNDERA_POOL_SIZE_MAX_TOTAL = "kundera.pool.size.max.total";
}
