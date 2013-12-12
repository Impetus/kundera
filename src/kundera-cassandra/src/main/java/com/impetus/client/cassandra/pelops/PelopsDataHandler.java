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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Provides Pelops utility methods for data held in Column family based stores.
 * 
 * @author amresh.singh
 */
final class PelopsDataHandler extends CassandraDataHandlerBase implements CassandraDataHandler
{

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

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(m.getEntityClazz());

        // For secondary tables.
        List<String> secondaryTables = ((DefaultEntityAnnotationProcessor) managedType.getEntityAnnotation())
                .getSecondaryTablesName();
        secondaryTables.add(m.getTableName());
        Object e = null;
//        e = PelopsUtils.initialize(m, e, null);

        for (String tableName : secondaryTables)
        {
            Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftColumnOrSuperColumns = selector
                    .getColumnOrSuperColumnsFromRows(new ColumnParent(tableName), rowKeys,
                            Selector.newColumnsPredicateAll(true, 10000), consistencyLevel);

            for (ByteBuffer key : thriftColumnOrSuperColumns.keySet())
            {
                if (!thriftColumnOrSuperColumns.get(key).isEmpty())
                {
                    ThriftRow tr = new ThriftRow();
                    tr.setId(rowKey);
                    tr.setColumnFamilyName(tableName);

                    tr = thriftTranslator.translateToThriftRow(thriftColumnOrSuperColumns, m.isCounterColumnType(),
                            m.getType(), tr);

                    e = populateEntity(tr, m, CassandraUtilities.getEntity(e), relationNames, isWrapReq);
                }
            }
        }
        
        return e;
//        if (e != null  && PropertyAccessorHelper.getId(e, m) != null )
//        {
//            return isWrapReq && !relations.isEmpty() ? new EnhanceEntity(e, PropertyAccessorHelper.getId(e, m),
//                    relations) : e;
//        }
//        else
//        {
//            return null;
//        }
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
