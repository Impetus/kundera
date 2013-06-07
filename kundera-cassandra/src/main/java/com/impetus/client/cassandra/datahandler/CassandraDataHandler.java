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
package com.impetus.client.cassandra.datahandler;

import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;

import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Defines low level translation methods for Cassandra.
 * 
 * @author amresh.singh
 */
public interface CassandraDataHandler
{

    /**
     * From thrift row.
     * 
     * @param <E>
     *            the element type
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param tr
     *            the tr
     * @return the e
     * @throws Exception
     *             the exception
     */
    <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception;

    /**
     * From thrift row.
     * 
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param consistencyLevel
     *            the consistency level
     * @param rowIds
     *            the row ids
     * @return the list
     * @throws Exception
     *             the exception
     */
    List<Object> fromThriftRow(Class<?> clazz, EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            ConsistencyLevel consistencyLevel,  boolean isCql3Enabled,Object conn, Object... rowIds) throws Exception;

    /**
     * From thrift row.
     * 
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param rowKey
     *            the row key
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param consistencyLevel
     *            the consistency level
     * @return the object
     * @throws Exception
     *             the exception
     */
    Object fromThriftRow(Class<?> clazz, EntityMetadata m, Object rowKey, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel, boolean isCql3Enabled, Object conn) throws Exception;

    /**
     * Populate entity.
     * 
     * @param tr
     *            the tr
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @return the object
     */
    Object populateEntity(ThriftRow tr, EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            boolean isCql3Enabled);

    /**
     * @param e
     * @param id
     * @param m
     * @param columnFamily
     * @return
     * @throws Exception
     */
    ThriftRow toThriftRow(Object e, Object id, EntityMetadata m, String columnFamily) throws Exception;
}
