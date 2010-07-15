/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.db;

import java.util.List;

import com.impetus.kundera.CassandraClient;
import com.impetus.kundera.metadata.EntityMetadata;

/**
 * Interface used to interact with Cassandra Clients. This works as a bridge
 * between Entity objects and corresponding ThrftFamily objects.
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public interface DataAccessor {

    /**
     * Write.
     * 
     * @param client
     *            the client
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     * 
     * @throws Exception
     *             the exception
     */
    void write(CassandraClient client, EntityMetadata metadata, Object object) throws Exception;

    /**
     * Read.
     * 
     * @param client
     *            the client
     * @param metadata
     *            the metadata
     * @param clazz
     *            the clazz
     * @param key
     *            the key
     * 
     * @return the C
     * 
     * @throws Exception
     *             the exception
     */
    <C> C read(CassandraClient client, EntityMetadata metadata, Class<C> clazz, String key) throws Exception;

    /**
     * Read.
     * 
     * @param client
     *            the client
     * @param metadata
     *            the metadata
     * @param clazz
     *            the clazz
     * @param key
     *            the key
     * 
     * @return the list< c>
     * 
     * @throws Exception
     *             the exception
     */
    <C> List<C> read(CassandraClient client, EntityMetadata metadata, Class<C> clazz, String... key) throws Exception;

    /**
     * Delete.
     * 
     * @param client
     *            the client
     * @param metadata
     *            the metadata
     * @param key
     *            the key
     * 
     * @throws Exception
     *             the exception
     */
    void delete(CassandraClient client, EntityMetadata metadata, String key) throws Exception;
}
