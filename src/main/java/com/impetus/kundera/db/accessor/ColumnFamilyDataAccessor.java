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
package com.impetus.kundera.db.accessor;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * DataAccessor implementation for Cassandra's ColumnFamily.
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public final class ColumnFamilyDataAccessor extends BaseDataAccessor<Column> {

	/** log for this class. */
	private static Log log = LogFactory.getLog(ColumnFamilyDataAccessor.class);

	/**
	 * Instantiates a new column family data accessor.
	 * 
	 * @param em
	 *            the em
	 */
	public ColumnFamilyDataAccessor(EntityManagerImpl em) {
		super(em);
	}

	/*
	 * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
	 * com.impetus.kundera.metadata.EntityMetadata, java.lang.String)
	 */
	@Override
	public <E> E read(Class<E> clazz, EntityMetadata m, String id)
			throws Exception {
		log.debug("Cassandra >> Read >> " + clazz.getName() + "_" + id);

		String keyspace = m.getKeyspaceName();
		String family = m.getColumnFamilyName();

		 return getEntityManager().getClient().loadColumns(getEntityManager(),clazz,keyspace, family, id,m);
		// load column from DB
		//TODO uncomment
//		List<Column> columns =

//		E e;
//		HBaseClient hBaseClient = new HBaseClient();
//		HBaseData data = hBaseClient.read(m.getEntityClazz().getSimpleName().toLowerCase(), family, id, new String[0]);
//
//		e = onLoadFromHBase(clazz, data, m, id);
		//TODO uncomment.
		/*// check for empty
		if (null == columns || columns.size() == 0) {
			e = null;
		} else {
		    e = fromThriftRow(clazz, m, this.new ThriftRow(id, family, columns));
		}*/
		
//		return e;
	}

	/*
	 * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
	 * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
	 */
	@Override
	public <E> List<E> read(Class<E> clazz, EntityMetadata m, String... ids)
			throws Exception {
		log.debug("Cassandra >> Read >> " + clazz.getName() + "_("
				+ Arrays.asList(ids) + ")");

		String keyspace = m.getKeyspaceName();
		String family = m.getColumnFamilyName();
		
		return getEntityManager().getClient().loadColumns(getEntityManager(), clazz, keyspace, family, m, ids);

//		List<E> entities = new ArrayList<E>();
//
//		for(String id : ids) {
//		HBaseClient hBaseClient = new HBaseClient();
//		HBaseData data = hBaseClient.read(m.getEntityClazz().getSimpleName().toLowerCase(), family, id, new String[0]);
//		   entities.add(onLoadFromHBase(clazz, data, m, id));
//		return entities;
//		}
	}

	/*
	 * @seecom.impetus.kundera.db.DataAccessor#write(com.impetus.kundera.proxy.
	 * EnhancedEntity, com.impetus.kundera.metadata.EntityMetadata)
	 */
	@Override
	public void write(EnhancedEntity e, EntityMetadata m) throws Exception {
		String entityName = e.getEntity().getClass().getName();
		String id = e.getId();

		log.debug("Cassandra >> Write >> " + entityName + "_" + id);

		String keyspace = m.getKeyspaceName();
		String family = m.getColumnFamilyName();

//		BaseDataAccessor<Column>.ThriftRow tf = toThriftRow(e, m);
		getEntityManager().getClient().writeColumns(keyspace, family, // columnFamily
				id, // row id
				m.getColumnsAsList(), e// list of columns
				);
//		HBaseClient hBaseClient = new HBaseClient();
//		hBaseClient.write(m.getEntityClazz().getSimpleName().toLowerCase(), family, id, m.getColumnsAsList(), e);
	}

}
