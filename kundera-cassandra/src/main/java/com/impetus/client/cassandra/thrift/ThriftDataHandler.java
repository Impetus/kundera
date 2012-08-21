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
package com.impetus.client.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.Validation;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper.ColumnFamilyType;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Data handler for Thrift Clients 
 * @author amresh.singh
 */
public final class ThriftDataHandler extends CassandraDataHandlerBase implements CassandraDataHandler
{
    Cassandra.Client cassandra_client;
    
    public ThriftDataHandler(Cassandra.Client cassandra_client) {
        this.cassandra_client = cassandra_client;
    }
    
    @Override
    public List<Object> fromThriftRow(Class<?> clazz, EntityMetadata m, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel, Object... rowIds) throws Exception
    {
        return super.fromThriftRow(clazz, m, relationNames, isWrapReq, consistencyLevel, rowIds);
    }  
    
    @Override
    public Object fromThriftRow(Class<?> clazz, EntityMetadata m, String rowKey, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel) throws Exception
    {        

        List<String> superColumnNames = m.getEmbeddedColumnFieldNames();
        Object e = null;

        if (!superColumnNames.isEmpty())
        {
            if (m.isCounterColumnType())
            {
                
                List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);
                rowKeys.add(ByteBufferUtil.bytes(rowKey));        
                
                
                SlicePredicate predicate = new SlicePredicate();
                predicate.setSlice_range(new SliceRange(Bytes.EMPTY.getBytes(), Bytes.EMPTY.getBytes(), true, 10000));
                
                Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns = cassandra_client
                        .multiget_slice(rowKeys, new ColumnParent(m.getTableName()), predicate, consistencyLevel);               
                
                List<CounterSuperColumn> thriftCounterSuperColumns = ThriftDataResultHelper.fetchDataFromThriftResult(
                        thriftColumnOrSuperColumns, ColumnFamilyType.COUNTER_SUPER_COLUMN);               
                
                if (thriftCounterSuperColumns != null)
                {
                    e = fromCounterSuperColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null, null,
                            null, thriftCounterSuperColumns), relationNames, isWrapReq);
                }
            }
            else
            {
             
               SlicePredicate predicate = new SlicePredicate();
               predicate.setSlice_range(new SliceRange(Bytes.EMPTY.getBytes(), Bytes.EMPTY.getBytes(), true, 10000));
                
                List<ColumnOrSuperColumn> columnOrSuperColumns = cassandra_client.get_slice(
                        ByteBuffer.wrap(rowKey.getBytes()), new ColumnParent(m.getTableName()), predicate,
                        consistencyLevel);

                List<SuperColumn> thriftSuperColumns = ThriftDataResultHelper.fetchDataFromThriftResult(
                        columnOrSuperColumns, ColumnFamilyType.SUPER_COLUMN);

                e = fromSuperColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null,
                        thriftSuperColumns, null, null), relationNames, isWrapReq);
            }
        }
        else
        {
            List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);

            ByteBuffer rKeyAsByte = ByteBufferUtil.bytes(rowKey);
            rowKeys.add(ByteBufferUtil.bytes(rowKey));

            SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(new SliceRange(Bytes.EMPTY.getBytes(), Bytes.EMPTY.getBytes(), true, 10000));
            
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnOrSuperColumnsFromRow = cassandra_client
                    .multiget_slice(rowKeys, new ColumnParent(m.getTableName()), predicate, consistencyLevel);
            
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
    
    @Override
    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception {
        return super.fromThriftRow(clazz, m, tr);
    }    
    
}
