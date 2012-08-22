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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper.ColumnFamilyType;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;

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
    
    /**
     * From thrift row.
     * 
     * @param selector
     *            the selector
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
     * @return the object
     * @throws Exception
     *             the exception
     */
    
    @Override
    public Object fromThriftRow(Class<?> clazz, EntityMetadata m, String rowKey,
            List<String> relationNames, boolean isWrapReq, ConsistencyLevel consistencyLevel) throws Exception
    {
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(m.getPersistenceUnit()));
        
        List<String> superColumnNames = m.getEmbeddedColumnFieldNames();
        Object e = null;

        if (!superColumnNames.isEmpty())
        {
            if (m.isCounterColumnType())
            {
                
                List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);
                rowKeys.add(ByteBufferUtil.bytes(rowKey));
                Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns = selector
                        .getColumnOrSuperColumnsFromRows(new ColumnParent(m.getTableName()), rowKeys,
                                Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);
                
                List<CounterSuperColumn> thriftCounterSuperColumns = ThriftDataResultHelper.transformThriftResultAndAddToList(thriftColumnOrSuperColumns, ColumnFamilyType.COUNTER_SUPER_COLUMN);
                
                if (thriftCounterSuperColumns != null)
                {
                    e = fromCounterSuperColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null, null,
                            null, thriftCounterSuperColumns), relationNames, isWrapReq);
                }
            }
            else
            {
                List<SuperColumn> thriftSuperColumns = selector.getSuperColumnsFromRow(m.getTableName(), rowKey,
                        Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);
                e = fromSuperColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null,
                        thriftSuperColumns, null, null), relationNames, isWrapReq);
            }
        }
        else
        {
            List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);

            ByteBuffer rKeyAsByte = ByteBufferUtil.bytes(rowKey);
            rowKeys.add(ByteBufferUtil.bytes(rowKey));

            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnOrSuperColumnsFromRow = selector
                    .getColumnOrSuperColumnsFromRows(new ColumnParent(m.getTableName()), rowKeys,
                            Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);

            List<ColumnOrSuperColumn> colList = columnOrSuperColumnsFromRow.get(rKeyAsByte);
            if (m.isCounterColumnType())
            {
                List<CounterColumn> thriftColumns = new ArrayList<CounterColumn>(colList.size());
                for (ColumnOrSuperColumn col : colList)
                {
                    if (col.super_column == null)
                    {
                        thriftColumns.add(col.getCounter_column());
                    }
                    else
                    {
                        thriftColumns.addAll(col.getCounter_super_column().getColumns());
                    }

                }

                e = fromCounterColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null, null,
                        thriftColumns, null), relationNames, isWrapReq);
            }
            else
            {
                List<Column> thriftColumns = new ArrayList<Column>(colList.size());
                for (ColumnOrSuperColumn col : colList)
                {
                    if (col.super_column == null)
                    {
                        thriftColumns.add(col.getColumn());
                    }
                    else
                    {
                        thriftColumns.addAll(col.getSuper_column().getColumns());
                    }

                }

                e = fromColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), thriftColumns, null, null,
                        null), relationNames, isWrapReq);
            }
        }
        return e;
    }

    /**
     * From thrift row.
     * 
     * @param selector
     *            the selector
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param isWrapReq
     *            the is wrap req
     * @param rowIds
     *            the row ids
     * @return the list
     * @throws Exception
     *             the exception
     */
    @Override
    public List<Object> fromThriftRow(Class<?> clazz, EntityMetadata m, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel, Object... rowIds) throws Exception
    {
        return super.fromThriftRow(clazz, m, relationNames, isWrapReq, consistencyLevel, rowIds);
    }     
    
    @Override
    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception {
        return super.fromThriftRow(clazz, m, tr);
    }  
    
    @Override
    public Object fromColumnThriftRow(Class<?> clazz, EntityMetadata m, ThriftRow thriftRow, List<String> relationNames,
            boolean isWrapperReq) throws Exception {
        return super.fromColumnThriftRow(clazz, m, thriftRow, relationNames, isWrapperReq);
    }
    
    @Override
    public Object fromCounterColumnThriftRow(Class<?> clazz, EntityMetadata m, ThriftRow thriftRow,
            List<String> relationNames, boolean isWrapperReq) throws Exception {
        return super.fromCounterColumnThriftRow(clazz, m, thriftRow, relationNames, isWrapperReq);
    }
    

}
