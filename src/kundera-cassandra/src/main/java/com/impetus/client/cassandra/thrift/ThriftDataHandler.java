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
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.scale7.cassandra.pelops.Bytes;

import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftClientFactory.Connection;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Data handler for Thrift Clients
 * 
 * @author amresh.singh
 */
/**
 * @author vivek.mishra
 * 
 */
public final class ThriftDataHandler extends CassandraDataHandlerBase implements CassandraDataHandler
{
    private final ThriftClient thriftClient;

    public ThriftDataHandler(final ThriftClient thriftClient)
    {
        super(thriftClient);
        this.thriftClient = thriftClient;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase#
     * fromThriftRow(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String,
     * java.util.List, boolean, org.apache.cassandra.thrift.ConsistencyLevel)
     */
    @Override
    public Object fromThriftRow(Class<?> clazz, EntityMetadata m, Object rowKey, List<String> relationNames,
            boolean isWrapReq, ConsistencyLevel consistencyLevel) throws Exception
    {
        // List<String> superColumnNames = m.getEmbeddedColumnFieldNames();

        Object e = null;
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(Bytes.EMPTY.getBytes(), Bytes.EMPTY.getBytes(), true, 10000));

        ByteBuffer key = ByteBuffer.wrap(PropertyAccessorHelper.toBytes(rowKey, m.getIdAttribute().getJavaType()));
        Connection conn = thriftClient.getConection();
        try
        {
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());

            AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(m.getEntityClazz());

            // For secondary tables.
            List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                    .getSecondaryTablesName();
            secondaryTables.add(m.getTableName());

//            e = PelopsUtils.initialize(m, e, null);

            for (String tableName : secondaryTables)
            {
                List<ColumnOrSuperColumn> columnOrSuperColumns = conn.getClient().get_slice(key,
                        new ColumnParent(tableName), predicate, consistencyLevel);

                Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
                thriftColumnOrSuperColumns.put(key, columnOrSuperColumns);
                if (!columnOrSuperColumns.isEmpty())
                {
                    e = populateEntityFromSlice(m, relationNames, isWrapReq, CassandraUtilities.getEntity(e) , thriftColumnOrSuperColumns);
                }
            }
//            if (e != null  && PropertyAccessorHelper.getId(e, m) != null )
//            {
//                return isWrapReq && !relations.isEmpty() ? new EnhanceEntity(e, PropertyAccessorHelper.getId(e, m),
//                        relations) : e;
//            }
//            else
//            {
//                return null;
//            }
            return e;
        }
        finally
        {
            thriftClient.releaseConnection(conn);
        }
    }

    @Override
    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception
    {
        return super.fromThriftRow(clazz, m, tr);
    }

    /**
     * @param m
     * @param relationNames
     * @param isWrapReq
     * @param e
     * @param columnOrSuperColumnsFromRow
     * @return
     * @throws CharacterCodingException
     */
    private Object populateEntityFromSlice(EntityMetadata m, List<String> relationNames, boolean isWrapReq, Object e,
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnOrSuperColumnsFromRow)
            throws CharacterCodingException
    {
        ThriftDataResultHelper dataGenerator = new ThriftDataResultHelper();
        for (ByteBuffer key : columnOrSuperColumnsFromRow.keySet())
        {
            Object id = PropertyAccessorHelper.getObject(m.getIdAttribute().getJavaType(), key.array());
            ThriftRow tr = new ThriftRow();
            tr.setColumnFamilyName(m.getTableName());
            tr.setId(id);
            tr = dataGenerator.translateToThriftRow(columnOrSuperColumnsFromRow, m.isCounterColumnType(), m.getType(),
                    tr);
            e = populateEntity(tr, m, e, relationNames, isWrapReq);
        }
        return e;
    }
}
