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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.db.accessor.ColumnFamilyDataAccessor;
import com.impetus.kundera.db.accessor.SuperColumnFamilyDataAccessor;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;

/**
 * The Class DataManager.
 * 
 * @author animesh.kumar
 */
public class DataManager {

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(EntityManagerImpl.class);

    /** instance of DataAccessors. */
    private DataAccessor dataAccessorForColumnFamily;

    /** The data accessor for super column family. */
    private DataAccessor dataAccessorForSuperColumnFamily;

    /** The em. */
    private EntityManagerImpl em;

    /**
     * The Constructor.
     * 
     * @param em
     *            the em
     */
    public DataManager(EntityManagerImpl em) {
        super();
        this.em = em;
        dataAccessorForColumnFamily = new ColumnFamilyDataAccessor();
        dataAccessorForSuperColumnFamily = new SuperColumnFamilyDataAccessor();
    }

    /**
     * Persist.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * 
     * @throws Exception
     *             the exception
     */
    public void persist(EntityMetadata metadata, Object entity) throws Exception {
        getDataAccessor(metadata).write(em.getClient(), metadata, entity);
    }

    /**
     * Find.
     * 
     * @param metadata
     *            the metadata
     * @param clazz
     *            the clazz
     * @param key
     *            the key
     * 
     * @return the t
     * 
     * @throws Exception
     *             the exception
     */
    public <T> T find(EntityMetadata metadata, Class<T> clazz, String key) throws Exception {
        return getDataAccessor(metadata).read(em.getClient(), metadata, clazz, key);
    }

    /**
     * Find.
     * 
     * @param metadata
     *            the metadata
     * @param clazz
     *            the clazz
     * @param keys
     *            the keys
     * 
     * @return the list< t>
     * 
     * @throws Exception
     *             the exception
     */
    public <T> List<T> find(EntityMetadata metadata, Class<T> clazz, String... keys) throws Exception {
        return getDataAccessor(metadata).read(em.getClient(), metadata, clazz, keys);
    }

    /**
     * Removes the.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * @param key
     *            the key
     * 
     * @throws Exception
     *             the exception
     */
    public void remove(EntityMetadata metadata, Object entity, String key) throws Exception {
        getDataAccessor(metadata).delete(em.getClient(), metadata, key);
    }

    /**
     * Gets the data accessor.
     * 
     * @param metadata
     *            the metadata
     * 
     * @return the data accessor
     */
    private DataAccessor getDataAccessor(EntityMetadata metadata) {
        EntityMetadata.Type type = metadata.getType();
        if (type.equals(EntityMetadata.Type.COLUMN_FAMILY)) {
            log.debug("DataAccessor for @Entity " + metadata.getEntityClazz().getName() + " is " + dataAccessorForColumnFamily.getClass().getName());
            return dataAccessorForColumnFamily;
        }

        else if (type.equals(EntityMetadata.Type.SUPER_COLUMN_FAMILY)) {
            log.debug("DataAccessor for @Entity " + metadata.getEntityClazz().getName() + " is " + dataAccessorForSuperColumnFamily.getClass().getName());
            return dataAccessorForSuperColumnFamily;
        }

        return null;
    }
}
