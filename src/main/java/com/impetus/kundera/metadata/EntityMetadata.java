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
package com.impetus.kundera.metadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author animesh.kumar
 * 
 */
public class EntityMetadata {

	/** type */
	Type type;

	/** class corresponding to this meta. */
	Class<?> entityClazz;

	/** ColumnFamily. */
	String columnFamilyName;

	/** field that keeps row identifier. */
	Field idProperty;

	/** columnMeta map. */
	Map<String, Column> columnsMap;

	/** supercolumn map. */
	Map<String, SuperColumn> superColumnsMap;

	/** document index boost, lucene specific */
	float indexBoost = 1.0f;

	String indexName;
	
	boolean isIndexable = true; // default is indexable

	List<PropertyIndex> indexPrperties;

	/**
	 * Instantiates a new metadata.
	 */
	public EntityMetadata(Class<?> entityClazz) {
		this.entityClazz = entityClazz;
		columnsMap = new HashMap<String, Column>();
		superColumnsMap = new HashMap<String, SuperColumn>();
		indexPrperties = new ArrayList<PropertyIndex>();
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public Class<?> getEntityClazz() {
		return entityClazz;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}

	public Field getIdProperty() {
		return idProperty;
	}

	public void setIdProperty(Field idProperty) {
		this.idProperty = idProperty;
	}

	public Map<String, Column> getColumnsMap() {
		return columnsMap;
	}

	public Map<String, SuperColumn> getSuperColumnsMap() {
		return superColumnsMap;
	}

	public Column getColumn(String key) {
		return columnsMap.get(key);
	}

	public SuperColumn getSuperColumn(String key) {
		return superColumnsMap.get(key);
	}

	public List<Column> getColumnsAsList() {
		return new ArrayList<Column>(columnsMap.values());
	}

	public List<SuperColumn> getSuperColumnsAsList() {
		return new ArrayList<SuperColumn>(superColumnsMap.values());
	}

	public List<String> getColumnFieldNames() {
		return new ArrayList<String>(columnsMap.keySet());
	}

	public List<String> getSuperColumnFieldNames() {
		return new ArrayList<String>(superColumnsMap.keySet());
	}

	public void addColumn(String key, Column column) {
		columnsMap.put(key, column);
	}

	public void addSuperColumn(String key, SuperColumn superColumn) {
		superColumnsMap.put(key, superColumn);
	}

	public void addIndexProperty(PropertyIndex index) {
		indexPrperties.add(index);
	}

	public List<PropertyIndex> getIndexProperties() {
		return indexPrperties;
	}

	public float getIndexBoost() {
		return indexBoost;
	}

	public void setIndexBoost(float indexBoost) {
		this.indexBoost = indexBoost;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public boolean isIndexable() {
		return isIndexable;
	}

	public void setIndexable(boolean isIndexable) {
		this.isIndexable = isIndexable;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("EntityMetadata [entityClazz=");
		builder.append(entityClazz);
		builder.append(", type=");
		builder.append(type);
		builder.append(", columnFamilyName=");
		builder.append(columnFamilyName);
		builder.append(", indexName=");
		builder.append(indexName);
		builder.append(", idProperty=");
		builder.append(idProperty);
		builder.append(", indexBoost=");
		builder.append(indexBoost);
		builder.append(", columnsMap=");
		builder.append(columnsMap);
		builder.append(", superColumnsMap=");
		builder.append(superColumnsMap);
		builder.append(", indexPrperties=");
		builder.append(indexPrperties);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * Represents Thrift Column
	 * 
	 * @author animesh.kumar
	 */
	public class Column {

		/** name of the column. */
		String name;

		/** field. */
		Field field;

		public Column(String name, Field field) {
			this.name = name;
			this.field = field;
		}

		public String getName() {
			return name;
		}

		public Field getField() {
			return field;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Column [field=");
			builder.append(field);
			builder.append(", name=");
			builder.append(name);
			builder.append("]");
			return builder.toString();
		}
	}

	/**
	 * Represents Thrift SuperColumn
	 * 
	 * @author animesh.kumar
	 */
	public class SuperColumn {

		/** The name. */
		String name;

		/** The columns. */
		List<Column> columns;

		public SuperColumn(String name) {
			this.name = name;
			columns = new ArrayList<Column>();
		}

		public String getName() {
			return name;
		}

		public List<Column> getColumns() {
			return columns;
		}

		public void addColumn(String name, Field field) {
			columns.add(new Column(name, field));
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("SuperColumn [name=");
			builder.append(name);
			builder.append(", columns=");
			builder.append(columns);
			builder.append("]");
			return builder.toString();
		}
	}

	/**
	 * Contains Index information of a field
	 * 
	 * @author animesh.kumar
	 */
	public class PropertyIndex {

		String name;
		Field property;
		float boost = 1.0f;

		/**
		 * @param field
		 */
		public PropertyIndex(Field property) {
			this.property = property;
			this.name = property.getName();
		}

		public PropertyIndex(Field property, String name) {
			this.property = property;
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public Field getProperty() {
			return property;
		}

		public float getBoost() {
			return boost;
		}

		public void setBoost(float boost) {
			this.boost = boost;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("PropertyIndex [name=");
			builder.append(name);
			builder.append(", property=");
			builder.append(property);
			builder.append(", boost=");
			builder.append(boost);
			builder.append("]");
			return builder.toString();
		}
	}

	/** Specifies the type of the metadata. */
	public static enum Type {

		/** Denotes that the Entity is related to a ColumnFamily */
		COLUMN_FAMILY {

			public boolean isColumnFamilyMetadata() {
				return true;
			}

			public boolean isSuperColumnFamilyMetadata() {
				return false;
			}

		},
		/** Denotes that the Entity is related to a SuperColumnFamily */
		SUPER_COLUMN_FAMILY {

			public boolean isColumnFamilyMetadata() {
				return false;
			}

			public boolean isSuperColumnFamilyMetadata() {
				return true;
			}

		};

		public abstract boolean isColumnFamilyMetadata();

		public abstract boolean isSuperColumnFamilyMetadata();
	}
}
