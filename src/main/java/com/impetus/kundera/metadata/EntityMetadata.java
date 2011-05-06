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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.FetchType;

import com.impetus.kundera.ejb.event.CallbackMethod;
import com.impetus.kundera.loader.DBType;

/**
 * The Class EntityMetadata.
 * 
 * @author animesh.kumar
 */
public final class EntityMetadata {

	/** type. */
	private Type type;

	/** class corresponding to this meta. */
	private Class<?> entityClazz;

	/** ColumnFamily. */
	private String columnFamilyName;

	/** keyspace. */
	private String keyspaceName;

	/** field that keeps row identifier. */
	private Field idProperty;
	
	/** Column that keeps row identifier. */
	private Column idColumn;

	/** The read identifier method. */
	private Method readIdentifierMethod;

	/** The write identifier method. */
	private Method writeIdentifierMethod;

	/** columnMeta map. */
	private Map<String, Column> columnsMap = new HashMap<String, Column>();

	/** supercolumn map. */
	private Map<String, SuperColumn> superColumnsMap = new HashMap<String, SuperColumn>();

	/** document index boost, lucene specific. */
	private float indexBoost = 1.0f;

	/** The index name. */
	private String indexName;

	/** The is indexable. */
	private boolean isIndexable = true; // default is indexable

	/** The index prperties. */
	private List<PropertyIndex> indexPrperties = new ArrayList<PropertyIndex>();

	// entity listeners map
	// key=>ListenerAnnotations, like @PrePersist, @PreUpdate etc.;
	// value=>EntityLisntener Class and method
	/** The callback methods map. */
	private Map<Class<?>, List<? extends CallbackMethod>> callbackMethodsMap = new HashMap<Class<?>, List<? extends CallbackMethod>>();

	/** Relationship map, key=>property name, value=>relation. */
	private Map<String, Relation> relationsMap = new HashMap<String, Relation>();

	/** Cacheable?. */
	private boolean cacheable = false; // default is to not set second-level
										// cache
	
	private DBType dbType;

	/**
	 * The Enum ForeignKey.
	 */
	public static enum ForeignKey {
		ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY
	};

	/**
	 * Instantiates a new metadata.
	 * 
	 * @param entityClazz
	 *            the entity clazz
	 */
	public EntityMetadata(Class<?> entityClazz) {
		this.entityClazz = entityClazz;

	}

	/**
	 * Gets the type.
	 * 
	 * @return the type
	 */
	public Type getType() {
		return type;
	}

	/**
	 * Sets the type.
	 * 
	 * @param type
	 *            the new type
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Gets the entity clazz.
	 * 
	 * @return the entity clazz
	 */
	public Class<?> getEntityClazz() {
		return entityClazz;
	}

	/**
	 * Gets the column family name.
	 * 
	 * @return the column family name
	 */
	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	/**
	 * Sets the column family name.
	 * 
	 * @param columnFamilyName
	 *            the new column family name
	 */
	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}

	/**
	 * Gets the keyspace name.
	 * 
	 * @return the keyspace name
	 */
	public String getKeyspaceName() {
		return keyspaceName;
	}

	/**
	 * Sets the keyspace name.
	 * 
	 * @param keyspaceName
	 *            the new keyspace name
	 */
	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	/**
	 * Gets the id property.
	 * 
	 * @return the id property
	 */
	public Field getIdProperty() {
		return idProperty;
	}

	/**
	 * Sets the id property.
	 * 
	 * @param idProperty
	 *            the new id property
	 */
	public void setIdProperty(Field idProperty) {
		this.idProperty = idProperty;
	}	

	/**
	 * @return the idColumn
	 */
	public Column getIdColumn() {
		return idColumn;
	}

	/**
	 * @param idColumn the idColumn to set
	 */
	public void setIdColumn(Column idColumn) {
		this.idColumn = idColumn;
	}

	/**
	 * Gets the read identifier method.
	 * 
	 * @return the readIdentifierMethod
	 */
	public Method getReadIdentifierMethod() {
		return readIdentifierMethod;
	}

	/**
	 * Sets the read identifier method.
	 * 
	 * @param readIdentifierMethod
	 *            the readIdentifierMethod to set
	 */
	public void setReadIdentifierMethod(Method readIdentifierMethod) {
		this.readIdentifierMethod = readIdentifierMethod;
	}

	/**
	 * Gets the write identifier method.
	 * 
	 * @return the writeIdentifierMethod
	 */
	public Method getWriteIdentifierMethod() {
		return writeIdentifierMethod;
	}

	/**
	 * Sets the write identifier method.
	 * 
	 * @param writeIdentifierMethod
	 *            the writeIdentifierMethod to set
	 */
	public void setWriteIdentifierMethod(Method writeIdentifierMethod) {
		this.writeIdentifierMethod = writeIdentifierMethod;
	}

	/**
	 * Gets the columns map.
	 * 
	 * @return the columns map
	 */
	public Map<String, Column> getColumnsMap() {
		return columnsMap;
	}

	/**
	 * Gets the super columns map.
	 * 
	 * @return the super columns map
	 */
	public Map<String, SuperColumn> getSuperColumnsMap() {
		return superColumnsMap;
	}

	/**
	 * Gets the column.
	 * 
	 * @param key
	 *            the key
	 * 
	 * @return the column
	 */
	public Column getColumn(String key) {
		return columnsMap.get(key);
	}

	/**
	 * Gets the super column.
	 * 
	 * @param key
	 *            the key
	 * 
	 * @return the super column
	 */
	public SuperColumn getSuperColumn(String key) {
		return superColumnsMap.get(key);
	}

	/**
	 * Gets the columns as list.
	 * 
	 * @return the columns as list
	 */
	public List<Column> getColumnsAsList() {
		return new ArrayList<Column>(columnsMap.values());
	}

	/**
	 * Gets the super columns as list.
	 * 
	 * @return the super columns as list
	 */
	public List<SuperColumn> getSuperColumnsAsList() {
		return new ArrayList<SuperColumn>(superColumnsMap.values());
	}

	/**
	 * Gets the column field names.
	 * 
	 * @return the column field names
	 */
	public List<String> getColumnFieldNames() {
		return new ArrayList<String>(columnsMap.keySet());
	}

	/**
	 * Gets the super column field names.
	 * 
	 * @return the super column field names
	 */
	public List<String> getSuperColumnFieldNames() {
		return new ArrayList<String>(superColumnsMap.keySet());
	}

	/**
	 * Adds the column.
	 * 
	 * @param key
	 *            the key
	 * @param column
	 *            the column
	 */
	public void addColumn(String key, Column column) {
		columnsMap.put(key, column);
	}

	/**
	 * Adds the super column.
	 * 
	 * @param key
	 *            the key
	 * @param superColumn
	 *            the super column
	 */
	public void addSuperColumn(String key, SuperColumn superColumn) {
		superColumnsMap.put(key, superColumn);
	}

	/**
	 * Adds the index property.
	 * 
	 * @param index
	 *            the index
	 */
	public void addIndexProperty(PropertyIndex index) {
		indexPrperties.add(index);
	}

	/**
	 * Adds the relation.
	 * 
	 * @param property
	 *            the property
	 * @param relation
	 *            the relation
	 */
	public void addRelation(String property, Relation relation) {
		relationsMap.put(property, relation);
	}

	/**
	 * Gets the relation.
	 * 
	 * @param property
	 *            the property
	 * @return the relation
	 */
	public Relation getRelation(String property) {
		return relationsMap.get(property);
	}

	/**
	 * Gets the relations.
	 * 
	 * @return the relations
	 */
	public List<Relation> getRelations() {
		return new ArrayList<Relation>(relationsMap.values());
	}

	/**
	 * Gets the index properties.
	 * 
	 * @return the index properties
	 */
	public List<PropertyIndex> getIndexProperties() {
		return indexPrperties;
	}

	/**
	 * Gets the index boost.
	 * 
	 * @return the index boost
	 */
	public float getIndexBoost() {
		return indexBoost;
	}

	/**
	 * Sets the index boost.
	 * 
	 * @param indexBoost
	 *            the new index boost
	 */
	public void setIndexBoost(float indexBoost) {
		this.indexBoost = indexBoost;
	}

	/**
	 * Gets the index name.
	 * 
	 * @return the index name
	 */
	public String getIndexName() {
		return indexName;
	}

	/**
	 * Sets the index name.
	 * 
	 * @param indexName
	 *            the new index name
	 */
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	/**
	 * Checks if is indexable.
	 * 
	 * @return true, if is indexable
	 */
	public boolean isIndexable() {
		return isIndexable;
	}

	/**
	 * Sets the indexable.
	 * 
	 * @param isIndexable
	 *            the new indexable
	 */
	public void setIndexable(boolean isIndexable) {
		this.isIndexable = isIndexable;
	}

	/**
	 * Sets the callback methods map.
	 * 
	 * @param callbackMethodsMap
	 *            the callback methods map
	 */
	public void setCallbackMethodsMap(
			Map<Class<?>, List<? extends CallbackMethod>> callbackMethodsMap) {
		this.callbackMethodsMap = callbackMethodsMap;
	}

	/**
	 * Gets the callback methods map.
	 * 
	 * @return the callback methods map
	 */
	public Map<Class<?>, List<? extends CallbackMethod>> getCallbackMethodsMap() {
		return callbackMethodsMap;
	}

	/**
	 * Gets the callback methods.
	 * 
	 * @param event
	 *            the event
	 * 
	 * @return the callback methods
	 */
	public List<? extends CallbackMethod> getCallbackMethods(Class<?> event) {
		return this.callbackMethodsMap.get(event);
	}

	/**
	 * Checks if is cacheable.
	 * 
	 * @return the cacheable
	 */
	public boolean isCacheable() {
		return cacheable;
	}

	/**
	 * Sets the cacheable.
	 * 
	 * @param cacheable
	 *            the cacheable to set
	 */
	public void setCacheable(boolean cacheable) {
		this.cacheable = cacheable;
	}

	/* @see java.lang.Object#toString() */
	@Override
	public String toString() {
		int start = 0;
		StringBuilder builder = new StringBuilder();
		builder.append(entityClazz.getName() + " (\n");
		// builder.append("EntityMetadata [\n");

		// builder.append("\tentity:" + entityClazz.getName() + ",\n");
		builder.append("\tType: " + type + ",\n");
		builder.append("\tColumnFamily: " + columnFamilyName + ", \n");
		builder.append("\tKeyspace: " + keyspaceName + ",\n");
		builder.append("\tId: " + idProperty.getName() + ",\n");
		builder.append("\tReadIdMethod: " + readIdentifierMethod.getName()
				+ ",\n");
		builder.append("\tWriteIdMethod: " + writeIdentifierMethod.getName()
				+ ",\n");
		builder.append("\tCacheable: " + cacheable + ",\n");

		if (!columnsMap.isEmpty()) {
			builder.append("\tColumns (");
			for (Column col : columnsMap.values()) {
				if (start++ != 0) {
					builder.append(", ");
				}
				builder.append(col.getName());
			}
			builder.append("),\n");
		}

		if (!superColumnsMap.isEmpty()) {
			builder.append("\tSuperColumns (\n");
			for (SuperColumn col : superColumnsMap.values()) {
				builder.append("\t\t" + col.getName() + "(");

				start = 0;
				for (Column c : col.getColumns()) {
					if (start++ != 0) {
						builder.append(", ");
					}
					builder.append(c.getName());
				}
				builder.append(")\n");
			}
			builder.append("\t),\n");
		}

		if (!indexPrperties.isEmpty()) {
			// builder.append("\tIndexName: " + indexName + ",\n");
			// builder.append("\tIndexBoost: " + indexBoost + ",\n");

			builder.append("\tIndexes (");
			start = 0;
			for (PropertyIndex index : indexPrperties) {
				if (start++ != 0) {
					builder.append(", ");
				}
				builder.append(index.getName());
			}
			builder.append("),\n");
		}

		if (!callbackMethodsMap.isEmpty()) {
			builder.append("\tListeners (\n");
			for (Map.Entry<Class<?>, List<? extends CallbackMethod>> entry : callbackMethodsMap
					.entrySet()) {
				String key = entry.getKey().getSimpleName();
				for (CallbackMethod cbm : entry.getValue()) {
					builder.append("\t\t" + key + ": " + cbm + "\n");
				}
			}
			builder.append("\t)\n");
		}

		if (!relationsMap.isEmpty()) {
			builder.append("\tRelation (\n");
			for (Relation rel : relationsMap.values()) {
				builder.append("\t\t" + rel.getTargetEntity().getName() + "#"
						+ rel.getProperty().getName());
				builder.append(" (" + rel.getCascades());
				builder.append(", " + rel.getType());
				builder.append(", " + rel.fetchType);
				builder.append(")\n");
			}
			builder.append("\t)\n");
		}

		builder.append(")");
		return builder.toString();
	}

	/**
	 * Represents Thrift Column.
	 * 
	 * @author animesh.kumar
	 */
	public final class Column {

		/** name of the column. */
		private String name;

		/** field. */
		private Field field;

		/**
		 * Instantiates a new column.
		 * 
		 * @param name
		 *            the name
		 * @param field
		 *            the field
		 */
		public Column(String name, Field field) {
			this.name = name;
			this.field = field;
		}

		/**
		 * Gets the name.
		 * 
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * Gets the field.
		 * 
		 * @return the field
		 */
		public Field getField() {
			return field;
		}
	}

	/**
	 * Represents Thrift SuperColumn.
	 * 
	 * @author animesh.kumar
	 */
	public final class SuperColumn {

		/** The name. */
		private String name;

		/** The columns. */
		private List<Column> columns;

		/**
		 * Instantiates a new super column.
		 * 
		 * @param name
		 *            the name
		 */
		public SuperColumn(String name) {
			this.name = name;
			columns = new ArrayList<Column>();
		}

		/**
		 * Gets the name.
		 * 
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * Gets the columns.
		 * 
		 * @return the columns
		 */
		public List<Column> getColumns() {
			return columns;
		}

		/**
		 * Adds the column.
		 * 
		 * @param name
		 *            the name
		 * @param field
		 *            the field
		 */
		public void addColumn(String name, Field field) {
			columns.add(new Column(name, field));
		}
	}

	/**
	 * Contains Index information of a field.
	 * 
	 * @author animesh.kumar
	 */
	public final class PropertyIndex {

		/** The name. */
		private String name;

		/** The property. */
		private Field property;

		/** The boost. */
		private float boost = 1.0f;

		/**
		 * The Constructor.
		 * 
		 * @param property
		 *            the property
		 */
		public PropertyIndex(Field property) {
			this.property = property;
			this.name = property.getName();
		}

		/**
		 * Instantiates a new property index.
		 * 
		 * @param property
		 *            the property
		 * @param name
		 *            the name
		 */
		public PropertyIndex(Field property, String name) {
			this.property = property;
			this.name = name;
		}

		/**
		 * Gets the name.
		 * 
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * Gets the property.
		 * 
		 * @return the property
		 */
		public Field getProperty() {
			return property;
		}

		/**
		 * Gets the boost.
		 * 
		 * @return the boost
		 */
		public float getBoost() {
			return boost;
		}

		/**
		 * Sets the boost.
		 * 
		 * @param boost
		 *            the new boost
		 */
		public void setBoost(float boost) {
			this.boost = boost;
		}
	}

	/**
	 * Specifies the type of the metadata.
	 */
	public static enum Type {

		/** Denotes that the Entity is related to a ColumnFamily. */
		COLUMN_FAMILY {

			public boolean isColumnFamilyMetadata() {
				return true;
			}

			public boolean isSuperColumnFamilyMetadata() {
				return false;
			}
			
			public boolean isDocumentMetadata() {
				return false;
			}

		},

		/** Denotes that the Entity is related to a SuperColumnFamily. */
		SUPER_COLUMN_FAMILY {

			public boolean isColumnFamilyMetadata() {
				return false;
			}

			public boolean isSuperColumnFamilyMetadata() {
				return true;
			}
			
			public boolean isDocumentMetadata() {
				return false;
			}

		},
		
		/** Denotes that the Entity is related to a document based data-store. */
		DOCUMENT {

			public boolean isColumnFamilyMetadata() {
				return false;
			}

			public boolean isSuperColumnFamilyMetadata() {
				return false;
			}
			
			public boolean isDocumentMetadata() {
				return true;
			}

		};

		/**
		 * Checks if is column family metadata.
		 * 
		 * @return true, if is column family metadata
		 */
		public abstract boolean isColumnFamilyMetadata();

		/**
		 * Checks if is super column family metadata.
		 * 
		 * @return true, if is super column family metadata
		 */
		public abstract boolean isSuperColumnFamilyMetadata();
		
		/**
		 * Checks if is Document metadata. 
		 * @return true, if is Document metadata
		 */
		public abstract boolean isDocumentMetadata();
	}

	/**
	 * Class to hold class-method instances for EntityListeners.
	 * 
	 * @author animesh.kumar
	 */
	public final class ExternalCallbackMethod implements CallbackMethod {

		/** The clazz. */
		private Class<?> clazz;

		/** The method. */
		private Method method;

		/**
		 * Instantiates a new external callback method.
		 * 
		 * @param clazz
		 *            the clazz
		 * @param method
		 *            the method
		 */
		public ExternalCallbackMethod(Class<?> clazz, Method method) {
			this.clazz = clazz;
			this.method = method;
		}

		/*
		 * @see
		 * com.impetus.kundera.ejb.event.CallbackMethod#invoke(java.lang.Object)
		 */
		public void invoke(Object entity) throws IllegalArgumentException,
				IllegalAccessException, InvocationTargetException,
				InstantiationException {
			if (!method.isAccessible())
				method.setAccessible(true);
			method.invoke(clazz.newInstance(), new Object[] { entity });
		}

		/* @see java.lang.Object#toString() */
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append(clazz.getName() + "." + method.getName());
			return builder.toString();
		}
	}

	/**
	 * The Class InternalCallbackMethod.
	 * 
	 * @author animesh.kumar
	 */
	public final class InternalCallbackMethod implements CallbackMethod {

		/** The method. */
		private Method method;

		/**
		 * Instantiates a new internal callback method.
		 * 
		 * @param method
		 *            the method
		 */
		public InternalCallbackMethod(Method method) {
			this.method = method;
		}

		/*
		 * @see
		 * com.impetus.kundera.ejb.event.CallbackMethod#invoke(java.lang.Object)
		 */
		public void invoke(Object entity) throws IllegalArgumentException,
				IllegalAccessException, InvocationTargetException,
				InstantiationException {
			if (!method.isAccessible())
				method.setAccessible(true);
			method.invoke(entity, new Object[] {});
		}

		/* @see java.lang.Object#toString() */
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append(entityClazz.getName() + "." + method.getName());
			return builder.toString();
		}
	}

	/**
	 * The Class Relation.
	 */
	public final class Relation {

		/** The property. */
		private Field property;

		/** The target entity. */
		private Class<?> targetEntity;

		/** The property type. */
		private Class<?> propertyType;

		/** The fetch type. */
		private FetchType fetchType;

		/** The cascades. */
		private List<CascadeType> cascades;

		/** The optional. */
		private boolean optional;

		/** The mapped by. */
		private String mappedBy;

		/** The type. */
		private ForeignKey type;

		/**
		 * Instantiates a new relation.
		 * 
		 * @param property
		 *            the property
		 * @param targetEntity
		 *            the target entity
		 * @param propertyType
		 *            the property type
		 * @param fetchType
		 *            the fetch type
		 * @param cascades
		 *            the cascades
		 * @param optional
		 *            the optional
		 * @param mappedBy
		 *            the mapped by
		 * @param type
		 *            the type
		 */
		public Relation(Field property, Class<?> targetEntity,
				Class<?> propertyType, FetchType fetchType,
				List<CascadeType> cascades, boolean optional, String mappedBy,
				ForeignKey type) {
			super();
			this.property = property;
			this.targetEntity = targetEntity;
			this.propertyType = propertyType;
			this.fetchType = fetchType;
			this.cascades = cascades;
			this.optional = optional;
			this.mappedBy = mappedBy;
			this.type = type;
		}

		/**
		 * Gets the property.
		 * 
		 * @return the property
		 */
		public Field getProperty() {
			return property;
		}

		/**
		 * Gets the target entity.
		 * 
		 * @return the targetEntity
		 */
		public Class<?> getTargetEntity() {
			return targetEntity;
		}

		/**
		 * Gets the property type.
		 * 
		 * @return the propertyType
		 */
		public Class<?> getPropertyType() {
			return propertyType;
		}

		/**
		 * Gets the fetch type.
		 * 
		 * @return the fetchType
		 */
		public FetchType getFetchType() {
			return fetchType;
		}

		/**
		 * Gets the cascades.
		 * 
		 * @return the cascades
		 */
		public List<CascadeType> getCascades() {
			return cascades;
		}

		/**
		 * Checks if is optional.
		 * 
		 * @return the optional
		 */
		public boolean isOptional() {
			return optional;
		}

		/**
		 * Gets the mapped by.
		 * 
		 * @return the mappedBy
		 */
		public String getMappedBy() {
			return mappedBy;
		}

		/**
		 * Gets the type.
		 * 
		 * @return the type
		 */
		public ForeignKey getType() {
			return type;
		}

		/**
		 * Checks if is unary.
		 * 
		 * @return true, if is unary
		 */
		public boolean isUnary() {
			return type.equals(ForeignKey.ONE_TO_ONE)
					|| type.equals(ForeignKey.MANY_TO_ONE);
		}

		/**
		 * Checks if is collection.
		 * 
		 * @return true, if is collection
		 */
		public boolean isCollection() {
			return type.equals(ForeignKey.ONE_TO_MANY)
					|| type.equals(ForeignKey.MANY_TO_MANY);
		}
	}
	
	public DBType getDBType(){
		return dbType;
	}
	
	public void setDBType(DBType type) {
		this.dbType = type;
	}
}
