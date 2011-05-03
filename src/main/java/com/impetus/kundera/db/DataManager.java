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

import com.impetus.kundera.db.accessor.DocumentDataAccessor;
import com.impetus.kundera.db.accessor.ColumnFamilyDataAccessor;
import com.impetus.kundera.db.accessor.SuperColumnFamilyDataAccessor;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * This class handles all DB related requests.
 * 
 * @author animesh.kumar
 */
public class DataManager {

    /** instance of DataAccessors. */
    private DataAccessor accessorCF;

    /** The data accessor for super column family. */
    private DataAccessor accessorSCF;
    
    /** The data accessor for Document(document based data store). */
    private DataAccessor accessorDocument;

    /**
     * The Constructor.
     * 
     * @param em
     *            the EntityManager
     */
    public DataManager(EntityManagerImpl em) {
        accessorCF = new ColumnFamilyDataAccessor(em);
        accessorSCF = new SuperColumnFamilyDataAccessor(em);
        accessorDocument = new DocumentDataAccessor(em);
    }

    /**
	 * Persist an instance of EnhancedEntity.
	 * 
	 * @param e
	 *            EnhancedEntity
	 * @param m
	 *            Metadata
	 * @throws Exception
	 *             the exception
	 */
    public final void persist(EnhancedEntity e, EntityMetadata m) throws Exception {
        getDataAccessor(m).write(e, m);
    }

    /**
	 * Merge an instance of EnhancedEntity.
	 * 
	 * @param e
	 *            EnhancedEntity
	 * @param m
	 *            Metadata
	 * @return the enhanced entity
	 * @throws Exception
	 *             the exception
	 */
    public EnhancedEntity merge(EnhancedEntity e, EntityMetadata m) throws Exception {
        // 	TODO: improve this part. 
    	//	Should we not implement some merge on client level?
    	getDataAccessor(m).write(e, m);
        return e;
    }

    /**
	 * Remove an instance of EnhancedEntity.
	 * 
	 * @param e
	 *            EnhancedEntity
	 * @param m
	 *            Metadata
	 * @throws Exception
	 *             the exception
	 */
    public final void remove(EnhancedEntity e, EntityMetadata m) throws Exception {
    	getDataAccessor(m).delete(e, m);
    }

    /**
	 * Find entity of type clazz with primaryKey id.
	 * 
	 * @param <E>
	 *            Generics of entity
	 * @param clazz
	 *            Entity class
	 * @param m
	 *            Metadata
	 * @param id
	 *            Entity primary key
	 * @return Entity Object or null if none found
	 * @throws Exception
	 *             the exception
	 */
    public final <E> E find(Class<E> clazz, EntityMetadata m, String id) throws Exception {
    	return getDataAccessor(m).read(clazz, m, id);
    }

    /**
	 * Find a list of entities of type clazz with primaryKeys ids.
	 * 
	 * @param <E>
	 *            Generics of entity
	 * @param clazz
	 *            Entity class
	 * @param m
	 *            Metadata
	 * @param ids
	 *            the ids
	 * @return Entity Object
	 * @throws Exception
	 *             the exception
	 */
    public final <E> List<E> find(Class<E> clazz, EntityMetadata m, String... ids) throws Exception {
        return getDataAccessor(m).read(clazz, m, ids);
    }

    // Helper method to find appropriate DataAccessor
	/**
	 * Gets the data accessor.
	 * 
	 * @param metadata
	 *            the metadata
	 * @return the data accessor
	 */
    private DataAccessor getDataAccessor(EntityMetadata metadata) {

		EntityMetadata.Type type = metadata.getType();
		if (type.equals(EntityMetadata.Type.COLUMN_FAMILY)) {
			return accessorCF;
		} else if (type.equals(EntityMetadata.Type.SUPER_COLUMN_FAMILY)) {
			return accessorSCF;
		} else if (type.equals(EntityMetadata.Type.DOCUMENT)) {
			return accessorDocument;
		}

		return null;
	}
}
