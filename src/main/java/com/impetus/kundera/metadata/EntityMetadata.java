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

import com.impetus.kundera.ejb.event.CallbackMethod;

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

    /** field that keeps row identifier. */
    private Field idProperty;

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
	private Map<Class<?>, List<? extends CallbackMethod>> callbackMethodsMap = new HashMap<Class<?>, List<? extends CallbackMethod>>();
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

	public void setCallbackMethodsMap(Map<Class<?>, List<? extends CallbackMethod>> callbackMethodsMap) {
		this.callbackMethodsMap = callbackMethodsMap;
	}

	public Map<Class<?>, List<? extends CallbackMethod>> getCallbackMethodsMap() {
		return callbackMethodsMap;
	}

	public List<? extends CallbackMethod> getCallbackMethods(Class<?> event) {
		return this.callbackMethodsMap.get(event);
	}
	
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
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

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
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

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
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

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
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

        },

        /** Denotes that the Entity is related to a SuperColumnFamily. */
        SUPER_COLUMN_FAMILY {

            public boolean isColumnFamilyMetadata() {
                return false;
            }

            public boolean isSuperColumnFamilyMetadata() {
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
    }
	/**
	 * Class to hold class-method instances for EntityListeners
	 * 
	 * @author animesh.kumar
	 */
	public final class ExternalCallbackMethod implements CallbackMethod {
		private Class<?> clazz;
		private Method method;

		public ExternalCallbackMethod(Class<?> clazz, Method method) {
			this.clazz = clazz;
			this.method = method;
		}

		public void invoke(Object entity) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException {
			if (!method.isAccessible()) method.setAccessible(true);
			method.invoke(clazz.newInstance(), new Object[] {entity});
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ExternalCallBackMethod [clazz=");
			builder.append(clazz.getName());
			builder.append(", method=");
			builder.append(method.getName());
			builder.append("]");
			return builder.toString();
		}
	}

	/**
	 * @author animesh.kumar
	 *
	 */
	public final class InternalCallbackMethod implements CallbackMethod {
		private Method method;

		public InternalCallbackMethod(Method method) {
			this.method = method;
		}

		public void invoke(Object entity) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException {
			if (!method.isAccessible()) method.setAccessible(true);
			method.invoke(entity, new Object[] {});
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("InternalCallBackMethod [clazz=");
			builder.append(getEntityClazz().getName());
			builder.append(", method=");
			builder.append(method.getName());
			builder.append("]");
			return builder.toString();
		}
	}
	
}
