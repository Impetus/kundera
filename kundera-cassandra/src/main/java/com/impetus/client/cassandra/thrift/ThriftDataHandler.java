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
import java.util.Set;

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
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftDataResultHelper.ColumnFamilyType;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;

/**
 * Data handler for Thrift Clients
 * 
 * @author amresh.singh
 */
public final class ThriftDataHandler extends CassandraDataHandlerBase implements CassandraDataHandler
{    

    
    public ThriftDataHandler()
    {        
    }

    @Override
    public Object fromThriftRow(Class<?> clazz, EntityMetadata m, String rowKey, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel) throws Exception
    {

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());
        
//      List<String> superColumnNames = m.getEmbeddedColumnFieldNames();
      Set<String> superColumnAttribs = metaModel.getEmbeddables(m.getEntityClazz()).keySet(); 

//       List<String> superColumnNames = m.getEmbeddedColumnFieldNames();
        Object e = null;
        
        IPooledConnection conn = PelopsUtils.getCassandraConnection(m.getPersistenceUnit());
        Cassandra.Client cassandra_client = conn.getAPI();              
        cassandra_client.set_keyspace(m.getSchema());

        if (!superColumnAttribs.isEmpty())
        {
            if (m.isCounterColumnType())
            {

                List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);
                rowKeys.add(ByteBufferUtil.bytes(rowKey));

                SlicePredicate predicate = new SlicePredicate();
                predicate.setSlice_range(new SliceRange(Bytes.EMPTY.getBytes(), Bytes.EMPTY.getBytes(), true, 10000));

                Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns = cassandra_client
                        .multiget_slice(rowKeys, new ColumnParent(m.getTableName()), predicate, consistencyLevel);

                List<CounterSuperColumn> thriftCounterSuperColumns = thriftTranslator
                        .transformThriftResultAndAddToList(thriftColumnOrSuperColumns,
                                ColumnFamilyType.COUNTER_SUPER_COLUMN,null);

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

                List<SuperColumn> thriftSuperColumns = ThriftDataResultHelper.transformThriftResult(
                        columnOrSuperColumns, ColumnFamilyType.SUPER_COLUMN,null);

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

            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnOrSuperColumnsFromRow = cassandra_client.multiget_slice(
                    rowKeys, new ColumnParent(m.getTableName()), predicate, consistencyLevel);

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

                e = populateEntity(new ThriftRow(rowKey, m.getTableName(), thriftColumns, new ArrayList<SuperColumn>(0), new ArrayList<CounterColumn>(0),
                        new ArrayList<CounterSuperColumn>(0)), m, relationNames, isWrapReq);
            }
        }
        
        PelopsUtils.releaseConnection(conn);
        return e;
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

    @Override
    public Object fromCounterColumnThriftRow(Class<?> clazz, EntityMetadata m, ThriftRow thriftRow,
            List<String> relationNames, boolean isWrapperReq) throws Exception
    {
        return super.fromCounterColumnThriftRow(clazz, m, thriftRow, relationNames, isWrapperReq);
    }

    @Override
    public Object fromSuperColumnThriftRow(Class clazz, EntityMetadata m, ThriftRow tr, List<String> relationNames,
            boolean isWrapReq) throws Exception
    {
        return super.fromSuperColumnThriftRow(clazz, m, tr, relationNames, isWrapReq);
    }

    @Override
    public Object fromCounterSuperColumnThriftRow(Class clazz, EntityMetadata m, ThriftRow tr,
            List<String> relationNames, boolean isWrapReq) throws Exception
    {
        return super.fromCounterSuperColumnThriftRow(clazz, m, tr, relationNames, isWrapReq);
    }

}
