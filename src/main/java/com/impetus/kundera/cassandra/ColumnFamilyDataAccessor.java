/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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

package com.impetus.kundera.cassandra;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.db.accessor.BaseDataAccessor;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * DataAccessor implementation for Column Family based data-stores (Cassandra, HBase).
 * @author animesh.kumar
 * @since 0.1
 */
public final class ColumnFamilyDataAccessor extends BaseDataAccessor
{

    /** log for this class. */
    private static final Log log = LogFactory.getLog(ColumnFamilyDataAccessor.class);    

    /**
     * Instantiates a new column family data accessor.
     *
     * @param em
     *            the em
     */
    public ColumnFamilyDataAccessor(EntityManagerImpl em)
    {
        super(em);
    }

    /*
     * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String)
     */
    @Override
    public <E> E read(Class<E> clazz, EntityMetadata m, String id) throws Exception
    {
        log.debug("Column Family >> Read >> " + clazz.getName() + "_" + id);

        String keyspace = m.getSchema();
        String family = m.getTableName();

        return getEntityManager().getClient().loadData(getEntityManager(), clazz, keyspace, family, id, m);
    }

    /*
     * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    public <E> List<E> read(Class<E> clazz, EntityMetadata m, String... ids) throws Exception
    {        
        log.debug("Column Family >> Read >> " + clazz.getName() + "_(" + Arrays.asList(ids) + ")");

        String keyspace = m.getSchema();
        String family = m.getTableName();

        return getEntityManager().getClient().loadData(getEntityManager(), clazz, keyspace, family, m, ids);
    }    
    
    @Override
    public <E> List<E> read(Class<E> clazz, EntityMetadata m, Map<String, String> col) throws Exception
    {
        log.debug("Column Family >> Read >> " + clazz.getName());
        String keyspace = m.getSchema();
        String family = m.getTableName();
        return getEntityManager().getClient().loadData(getEntityManager(), clazz, m, col, keyspace, family);
    }    
    
    @Override
    public void write(EnhancedEntity e, EntityMetadata m) throws Exception
    {
        String entityName = e.getEntity().getClass().getName();
        String id = e.getId();

        log.debug("Column Family >> Write >> " + entityName + "_" + id);

        getEntityManager().getClient().writeData(getEntityManager(), e, m);
    }    

}
