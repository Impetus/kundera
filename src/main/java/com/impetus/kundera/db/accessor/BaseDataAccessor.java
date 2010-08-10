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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.db.DataAccessor;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * BaseDataAccessor.
 * 
 * @param <TF>
 *            Thrift data unit: Column or SuperColumn
 * @author animesh.kumar
 */
public abstract class BaseDataAccessor<TF> implements DataAccessor {

	/** log for this class. */
	private static Log log = LogFactory.getLog(BaseDataAccessor.class);

	/** The EntityManager. */
	private EntityManagerImpl em;

	/**
	 * Instantiates a new base data accessor.
	 * 
	 * @param em
	 *            the em
	 */
	public BaseDataAccessor(EntityManagerImpl em) {
		this.em = em;
	}

	/**
	 * Gets the entity manager.
	 * 
	 * @return EntityManager
	 */
	public EntityManagerImpl getEntityManager() {
		return em;
	}

	/*
	 * @see
	 * com.impetus.kundera.db.DataAccessor#delete(com.impetus.kundera.proxy.
	 * EnhancedEntity, com.impetus.kundera.metadata.EntityMetadata)
	 */
	@Override
	public void delete(EnhancedEntity e, EntityMetadata m) throws Exception {
		String entityName = e.getEntity().getClass().getName();
		String id = e.getId();

		log.debug("Cassandra >> Delete >> " + entityName + "_" + id);

		getEntityManager().getClient().delete(m.getKeyspaceName(),
				m.getColumnFamilyName(), id);
	}

	/**
	 * Creates a string representation of a set of foreign keys by combining
	 * them together separated by "~" character.
	 * 
	 * Note: Assumption is that @Id will never contain "~" character. Checks for
	 * this are not added yet.
	 * 
	 * @param foreignKeys
	 *            the foreign keys
	 * @return the string
	 */
	protected String serializeKeys(Set<String> foreignKeys) {
		if (null == foreignKeys || foreignKeys.isEmpty()) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		for (String key : foreignKeys) {
			if (sb.length() > 0) {
				sb.append(Constants.SEPARATOR);
			}
			sb.append(key);
		}
		return sb.toString();
	}

	/**
	 * Splits foreign keys into Set.
	 * 
	 * @param foreignKeys
	 *            the foreign keys
	 * @return the set
	 */
	protected Set<String> deserializeKeys(String foreignKeys) {
		Set<String> keys = new HashSet<String>();

		if (null == foreignKeys || foreignKeys.isEmpty()) {
			return keys;
		}

		String array[] = foreignKeys.split(Constants.SEPARATOR);
		for (String element : array) {
			keys.add(element);
		}
		return keys;
	}

	/**
	 * Utility class that represents a row in Cassandra DB.
	 * 
	 * @author animesh.kumar
	 */
	public class ThriftRow {

		/** Id of the row. */
		private String id;

		/** name of the family. */
		private String columnFamilyName;

		/** list of thrift columns from the row. */
		private List<TF> columns;

		/**
		 * default constructor.
		 */
		public ThriftRow() {
			columns = new ArrayList<TF>();
		}

		/**
		 * The Constructor.
		 * 
		 * @param id
		 *            the id
		 * @param columnFamilyName
		 *            the column family name
		 * @param columns
		 *            the columns
		 */
		public ThriftRow(String id, String columnFamilyName, List<TF> columns) {
			this.id = id;
			this.columnFamilyName = columnFamilyName;
			this.columns = columns;
		}

		/**
		 * Gets the id.
		 * 
		 * @return the id
		 */
		public String getId() {
			return id;
		}

		/**
		 * Sets the id.
		 * 
		 * @param id
		 *            the key to set
		 */
		public void setId(String id) {
			this.id = id;
		}

		/**
		 * Gets the column family name.
		 * 
		 * @return the columnFamilyName
		 */
		public String getColumnFamilyName() {
			return columnFamilyName;
		}

		/**
		 * Sets the column family name.
		 * 
		 * @param columnFamilyName
		 *            the columnFamilyName to set
		 */
		public void setColumnFamilyName(String columnFamilyName) {
			this.columnFamilyName = columnFamilyName;
		}

		/**
		 * Gets the columns.
		 * 
		 * @return the columns
		 */
		public List<TF> getColumns() {
			return columns;
		}

		/**
		 * Sets the columns.
		 * 
		 * @param columns
		 *            the columns to set
		 */
		public void setColumns(List<TF> columns) {
			this.columns = columns;
		}

		/**
		 * Adds the column.
		 * 
		 * @param column
		 *            the column
		 */
		public void addColumn(TF column) {
			columns.add(column);
		}
	}
}
