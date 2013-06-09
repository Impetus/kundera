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
package com.impetus.client.cassandra.pelops;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Provides Pelops utility methods for data held in Column family based stores.
 * 
 * @author amresh.singh
 */
final class PelopsDataHandler extends CassandraDataHandlerBase implements CassandraDataHandler
{
    /** The timestamp. */
    private long timestamp = System.currentTimeMillis();

    /** The log. */
    private static Log log = LogFactory.getLog(PelopsDataHandler.class);

    private final PelopsClient pelopsClient;

    /**
     * @param externalProperties
     */
    public PelopsDataHandler(final PelopsClient pelopsClient)
    {
        super(pelopsClient);
        this.pelopsClient = pelopsClient;
    }

    @Override
    public Object fromThriftRow(Class<?> clazz, EntityMetadata m, Object rowKey, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel) throws Exception
    {
        Selector selector = pelopsClient.getSelector();

        List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);
        rowKeys.add(ByteBuffer.wrap(PropertyAccessorHelper.toBytes(rowKey, m.getIdAttribute().getJavaType())));

        Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns = selector
                .getColumnOrSuperColumnsFromRows(new ColumnParent(m.getTableName()), rowKeys,
                        Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);

        ThriftRow tr = new ThriftRow();
        tr.setId(rowKey);
        tr.setColumnFamilyName(m.getTableName());

        tr = thriftTranslator
                .translateToThriftRow(thriftColumnOrSuperColumns, m.isCounterColumnType(), m.getType(), tr);

        return populateEntity(tr, m, relationNames, isWrapReq);
    }

    /** Translation Methods */

    @Override
    public List<Object> fromThriftRow(Class<?> clazz, EntityMetadata m, List<String> relationNames, boolean isWrapReq,
            ConsistencyLevel consistencyLevel, Object... rowIds) throws Exception
    {
        return super.fromThriftRow(clazz, m, relationNames, isWrapReq, consistencyLevel, rowIds);
    }

    @Override
    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception
    {
        return super.fromThriftRow(clazz, m, tr);
    }
}
