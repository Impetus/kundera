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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Column;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
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

		// load column from DB
		List<Column> columns = getEntityManager().getClient().loadColumns(
				keyspace, family, id);

		// check for empty
		if (null == columns || columns.size() == 0) {
			throw new PersistenceException("Entity not found for id: " + id);
		}

		E e = fromThriftRow(clazz, m, this.new ThriftRow(id, family, columns));
		return e;
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

		List<E> entities = new ArrayList<E>();

		// load columns from DB
		Map<String, List<Column>> map = getEntityManager().getClient()
				.loadColumns(keyspace, family, ids);

		// Iterate and populate entities
		for (Map.Entry<String, List<Column>> entry : map.entrySet()) {

			String id = entry.getKey();
			List<Column> columns = entry.getValue();

			if (entry.getValue().size() == 0) {
				log.debug("@Entity not found for id: " + id);
				continue;
			}

			E e = fromThriftRow(clazz, m, this.new ThriftRow(id, family,
					columns));
			entities.add(e);
		}

		return entities;
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

		BaseDataAccessor<Column>.ThriftRow tf = toThriftRow(e, m);

		getEntityManager().getClient().writeColumns(keyspace, family, // columnFamily
				tf.getId(), // row id
				tf.getColumns().toArray(new Column[0]) // list of columns
				);
	}

	// Helper method to convert ThriftRow to @Entity
	/**
	 * From thrift row.
	 * 
	 * @param <E>
	 *            the element type
	 * @param clazz
	 *            the clazz
	 * @param m
	 *            the m
	 * @param cr
	 *            the cr
	 * @return the e
	 * @throws Exception
	 *             the exception
	 */
	private <E> E fromThriftRow(Class<E> clazz, EntityMetadata m,
			BaseDataAccessor<Column>.ThriftRow cr) throws Exception {

		// Instantiate a new instance
		E e = clazz.newInstance();

		// Set row-key. Note: @Id is always String.
		PropertyAccessorHelper.set(e, m.getIdProperty(), cr.getId());

		// Iterate through each column
		for (Column c : cr.getColumns()) {
			String name = PropertyAccessorFactory.STRING.fromBytes(c.getName());
			byte[] value = c.getValue();

			if (null == value) {
				continue;
			}

			// check if this is a property?
			EntityMetadata.Column column = m.getColumn(name);
			if (null == column) {
				// it could be some relational column
				EntityMetadata.Relation relation = m.getRelation(name);

				if (relation == null) {
					continue;
				}

				String foreignKeys = PropertyAccessorFactory.STRING
						.fromBytes(value);
				Set<String> keys = deserializeKeys(foreignKeys);
				getEntityManager().getEntityResolver().populateForeignEntities(
						e, cr.getId(), relation, keys.toArray(new String[0]));
			}

			else {
				try {
					PropertyAccessorHelper.set(e, column.getField(), value);
				} catch (PropertyAccessException pae) {
					log.warn(pae.getMessage());
				}
			}
		}

		return e;
	}

	/**
	 * Helper method to convert @Entity to ThriftRow
	 * 
	 * @param e
	 *            the e
	 * @param m
	 *            the m
	 * @return the base data accessor. thrift row
	 * @throws Exception
	 *             the exception
	 */
	private BaseDataAccessor<Column>.ThriftRow toThriftRow(EnhancedEntity e,
			EntityMetadata m) throws Exception {

		// timestamp to use in thrift column objects
		long timestamp = System.currentTimeMillis();

		BaseDataAccessor<Column>.ThriftRow cr = this.new ThriftRow();

		// column-family name
		cr.setColumnFamilyName(m.getColumnFamilyName());

		// id
		cr.setId(e.getId());

		List<Column> columns = new ArrayList<Column>();

		// Iterate through each column-meta and populate that with field values
		for (EntityMetadata.Column column : m.getColumnsAsList()) {
			Field field = column.getField();
			String name = column.getName();
			try {
				byte[] value = PropertyAccessorHelper.get(e.getEntity(), field);
				columns.add(new Column(PropertyAccessorFactory.STRING
						.toBytes(name), value, timestamp));
			} catch (PropertyAccessException exp) {
				log.warn(exp.getMessage());
			}

		}

		// add foreign keys
		for (Map.Entry<String, Set<String>> entry : e.getForeignKeysMap()
				.entrySet()) {
			String property = entry.getKey();
			Set<String> foreignKeys = entry.getValue();

			String keys = serializeKeys(foreignKeys);
			if (null != keys) {
				columns.add(new Column(PropertyAccessorFactory.STRING
						.toBytes(property), PropertyAccessorFactory.STRING
						.toBytes(keys), timestamp));
			}
		}

		cr.setColumns(columns);
		return cr;
	}

}
