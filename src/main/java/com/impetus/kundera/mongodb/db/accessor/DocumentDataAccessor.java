/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.db.accessor;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.api.Document;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Relation;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * DataAccessor implementation for Document (documents based data stores like MongoDB and 
 * CouchDB
 * @author amresh.singh
 */
public final class DocumentDataAccessor extends BaseDataAccessor<Document> {
	private static Log log = LogFactory.getLog(DocumentDataAccessor.class);
	
	public DocumentDataAccessor(EntityManagerImpl em) {
		super(em);
	}

	
	@Override
	public void write(EnhancedEntity e, EntityMetadata m) throws Exception {
		String entityName = e.getEntity().getClass().getName();
		String id = e.getId();

		log.debug("Document >> Write >> " + entityName + "_" + id);		
		
		m.addColumn(m.getIdColumn().getName(), m.getIdColumn());	//Add PK column		
		getEntityManager().getClient().writeColumns(getEntityManager(), e, m);
		
	}
	
	@Override
	public <E> E read(Class<E> clazz, EntityMetadata m, String id)
			throws Exception {
		log.debug("Document >> Read >> " + clazz.getName() + "_" + id);

		String dbName = m.getKeyspaceName();	//Database name
		String documentName = m.getColumnFamilyName();	//Document name for document based data store
		m.addColumn(m.getIdColumn().getName(), m.getIdColumn());
		
		return getEntityManager().getClient().loadColumns(getEntityManager(),clazz,dbName, documentName, id,m);
	}

	/* (non-Javadoc)
	 * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class, com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
	 */
	@Override
	public <E> List<E> read(Class<E> clazz, EntityMetadata m, String... ids)
			throws Exception {
		log.debug("Document >> Read >> " + clazz.getName() + "_(" + Arrays.asList(ids) + ")");

		String dbName = m.getKeyspaceName();
		String documentName = m.getColumnFamilyName();
		m.addColumn(m.getIdColumn().getName(), m.getIdColumn());
		return getEntityManager().getClient().loadColumns(getEntityManager(), clazz, dbName, documentName, m, ids);
	}
	
	@Override
	public void delete(EnhancedEntity e, EntityMetadata m) throws Exception {
		String entityName = e.getEntity().getClass().getName();
		String id = e.getId();

		log.debug("Document >> Delete >> " + entityName + "_" + id);

		getEntityManager().getClient().delete(m.getIdColumn().getName(),
				m.getColumnFamilyName(), id);
	}

	@Override
	public EntityManagerImpl getEntityManager() {		
		return super.getEntityManager();
	}	

	
	@Override
	protected String serializeKeys(Set<String> foreignKeys) {		
		return super.serializeKeys(foreignKeys);
	}
	
	@Override
	protected Set<String> deserializeKeys(String foreignKeys) {		
		return super.deserializeKeys(foreignKeys);
	}
	
	
	
}
