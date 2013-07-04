/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.hbase.query;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.admin.HBaseDataHandler;
import com.impetus.client.hbase.query.HBaseQuery.QueryTranslator;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.IResultIterator;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * ResultIterator class, used to iterate over results.
 * 
 * @author Vivek.Mishra
 * 
 */
class ResultIterator<E> implements IResultIterator<E>
{
    private HBaseClient client;

    private EntityMetadata entityMetadata;

    private PersistenceDelegator persistenceDelegator;

    private HBaseDataHandler handler;

    private QueryTranslator translator;

    private List<String> columns;

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(ResultIterator.class);

    public ResultIterator(HBaseClient client, EntityMetadata m, PersistenceDelegator pd, int fetchSize,
            QueryTranslator translator, List<String> columns)
    {
        this.entityMetadata = m;
        this.client = client;
        this.persistenceDelegator = pd;
        this.handler = ((HBaseClient) client).getHandle();
        this.handler.setFetchSize(fetchSize);
        this.translator = translator;
        this.columns = columns;
        onQuery(m, client);
    }

    @Override
    public boolean hasNext()
    {
        boolean available = handler.hasNext();
        if (!available)
        {
            handler.reset();
        }

        return available;
    }

    @Override
    public E next()
    {
        E result = (E) handler.next(entityMetadata);
        if (!entityMetadata.isRelationViaJoinTable()
                && (entityMetadata.getRelationNames() == null || (entityMetadata.getRelationNames().isEmpty())))
        {
            return result;
        }
        else
        {
            return result = setRelationEntities(result, client, entityMetadata);
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove() over result iterator is not supported");
    }

    private E setRelationEntities(Object enhanceEntity, Client client, EntityMetadata m)
    {
        // Enhance entities can contain or may not contain relation.
        // if it contain a relation means it is a child
        // if it does not then it means it is a parent.
        E result = null;
        if (enhanceEntity != null)
        {

            if (!(enhanceEntity instanceof EnhanceEntity))
            {
                enhanceEntity = new EnhanceEntity(enhanceEntity, PropertyAccessorHelper.getId(enhanceEntity, m), null);
            }

            EnhanceEntity ee = (EnhanceEntity) enhanceEntity;

            result = (E) client.getReader().recursivelyFindEntities(ee.getEntity(), ee.getRelations(), m,
                    persistenceDelegator, false);
        }

        return result;
    }

    /**
     * Parses and translates query into HBase filter and invokes client's method
     * to return list of entities.
     * 
     * @param m
     *            Entity metadata
     * @param client
     *            hbase client
     * @return list of entities.
     */
    private void onQuery(EntityMetadata m, Client client)
    {

        try
        {
            // Called only in case of standalone entity.
            Map<Boolean, Filter> filter = translator.getFilter();
            String[] columnAsArr = getColumnsAsArray();

            if (isFindKeyOnly(m, columnAsArr))
            {
                this.handler.setFilter(new KeyOnlyFilter());
            }

            if (this.translator.isFindById() && (filter == null && columns == null))
            {
                handler.readData(m.getTableName(), m.getEntityClazz(), entityMetadata, translator.rowKey,
                        m.getRelationNames(), null);

            }
            if (translator.isFindById() && filter == null && columns != null)
            {
                handler.readDataByRange(m.getTableName(), m.getEntityClazz(), m, translator.rowKey, translator.rowKey,
                        columnAsArr, null);
            }
            if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
            {
                if (filter == null && !translator.isFindById())
                {
                    // means complete scan without where clause, scan all
                    // records.
                    // findAll.
                    if (translator.isRangeScan())
                    {
                        handler.readDataByRange(m.getTableName(), m.getEntityClazz(), m, translator.getStartRow(),
                                translator.getEndRow(), columnAsArr, null);
                    }
                    else
                    {
                        handler.readDataByRange(m.getTableName(), m.getEntityClazz(), m, null, null, columnAsArr, null);
                    }
                }
                else
                {
                    // means WHERE clause is present.

                    FilterList f = new FilterList();
                    if (filter != null && filter.values() != null && !filter.values().isEmpty())
                    {
                        f.addFilter(filter.values().iterator().next());
                    }
                    if (translator.isRangeScan())
                    {
                        handler.readDataByRange(m.getTableName(), m.getEntityClazz(), m, translator.getStartRow(),
                                translator.getEndRow(), columnAsArr, f);
                    }
                    else
                    {
                        // if range query. means query over id column. create
                        // range
                        // scan method.

                        handler.readData(m.getTableName(), entityMetadata.getEntityClazz(), entityMetadata, null,
                                m.getRelationNames(), f, columnAsArr);
                    }
                }
            }
        }
        catch (IOException ioex)
        {
            log.error("Error while executing query{} , Caused by:", ioex);
            throw new QueryHandlerException("Error while executing , Caused by:", ioex);
        }
    }

    private String[] getColumnsAsArray()
    {
        return columns.toArray(new String[columns.size()]);
    }

    /**
     * @param metadata
     * @param columns
     * @return
     */
    private boolean isFindKeyOnly(EntityMetadata metadata, String[] columns)
    {
        int noOFColumnsToFind = 0;
        boolean findIdOnly = false;
        if (columns != null)
        {
            for (String column : columns)
            {
                if (column != null)
                {
                    if (column.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
                    {
                        noOFColumnsToFind++;
                        findIdOnly = true;
                    }
                    else
                    {
                        noOFColumnsToFind++;
                        findIdOnly = false;
                    }
                }
            }
        }
        if (noOFColumnsToFind == 1 && findIdOnly)
        {
            return true;
        }
        return false;
    }

    @Override
    public List<E> next(int chunkSize)
    {
        throw new UnsupportedOperationException("Fetch in chunks is not yet supported over HBase!");
    }
}
