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
package com.impetus.kundera.metadata.model;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.JoinColumn;
import javax.persistence.PrimaryKeyJoinColumn;

import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.client.DBType;
import com.impetus.kundera.persistence.event.CallbackMethod;

/**
 * Holds metadata for entities.
 * 
 * @author animesh.kumar
 */

public final class EntityMetadata
{

    /*
     * Entity related metadata properties
     */

    /** class corresponding to this meta. */
    Class<?> entityClazz;

    /** Name of Persistence Object. */
    private String tableName;

    /** database name. */
    private String schema;

    /** Persistence unit of database this entity is to be persisted. */
    private String persistenceUnit;

    /** The index name. */
    private String indexName;

    /** The is indexable. */
    private boolean isIndexable = true; // default is indexable

    /** Cacheable?. */
    private boolean cacheable = false; // default is to not set second-level

    /*
     * Fields related metadata properties
     */

    /** Column that keeps row identifier. */
    private Column idColumn;

    /** The read identifier method. */
    private Method readIdentifierMethod;

    /** The write identifier method. */
    private Method writeIdentifierMethod;

    /** Maps of column name and their metadata. */
    private Map<String, Column> columnsMap = new HashMap<String, Column>();

    /** Map of embedded column names and their metadata. */
    private Map<String, EmbeddedColumn> embeddedColumnsMap = new HashMap<String, EmbeddedColumn>();

    /** The index prperties. */
    private List<PropertyIndex> indexPrperties = new ArrayList<PropertyIndex>();

    // entity listeners map
    // key=>ListenerAnnotations, like @PrePersist, @PreUpdate etc.;
    // value=>EntityLisntener Class and method
    /** The callback methods map. */
    private Map<Class<?>, List<? extends CallbackMethod>> callbackMethodsMap = new HashMap<Class<?>, List<? extends CallbackMethod>>();

    // TODO: Unused, remove this
    /** The embeddable collection. */
    private List<Class<?>> embeddableCollection = new ArrayList<Class<?>>();

    /** Relationship map, key=>property name, value=>relation. */
    private Map<String, Relation> relationsMap = new HashMap<String, Relation>();

    /** The db type. */
    private DBType dbType;

    /** type. */
    private Type type;

    /** The is relation via join table. */
    private boolean isRelationViaJoinTable;
    
    private List<String> relationNames;
    
    //Whether it contains One-To-Many relationship    
    private boolean isParent;
    
    /**
     * The Enum Type.
     */
    public static enum Type
    {

        /** Denotes that the Entity is related to a ColumnFamily. */
        COLUMN_FAMILY
        {

            public boolean isColumnFamilyMetadata()
            {
                return true;
            }

            public boolean isSuperColumnFamilyMetadata()
            {
                return false;
            }

            public boolean isDocumentMetadata()
            {
                return false;
            }

        },

        /** Denotes that the Entity is related to a SuperColumnFamily. */
        SUPER_COLUMN_FAMILY
        {

            public boolean isColumnFamilyMetadata()
            {
                return false;
            }

            public boolean isSuperColumnFamilyMetadata()
            {
                return true;
            }

            public boolean isDocumentMetadata()
            {
                return false;
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
         * 
         * @return true, if is Document metadata
         */
        public abstract boolean isDocumentMetadata();
    }

    /**
     * Gets the type.
     * 
     * @return the type
     */
    public Type getType()
    {
        return type;
    }

    /**
     * Sets the type.
     * 
     * @param type
     *            the new type
     */
    public void setType(Type type)
    {
        this.type = type;
    }

    /**
     * Instantiates a new metadata.
     * 
     * @param entityClazz
     *            the entity clazz
     */
    public EntityMetadata(Class<?> entityClazz)
    {
        this.entityClazz = entityClazz;

    }

    /**
     * Gets the entity clazz.
     * 
     * @return the entity clazz
     */
    public Class<?> getEntityClazz()
    {
        return entityClazz;
    }

    /**
     * Gets the table name.
     * 
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * Sets the table name.
     * 
     * @param tableName
     *            the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * Gets the schema.
     * 
     * @return the schema
     */
    public String getSchema()
    {
        return schema;
    }

    /**
     * Sets the schema.
     * 
     * @param schema
     *            the schema to set
     */
    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * Gets the persistence unit.
     * 
     * @return the persistenceUnit
     */
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    /**
     * Sets the persistence unit.
     * 
     * @param persistenceUnit
     *            the persistenceUnit to set
     */
    public void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }

    /**
     * Gets the id column.
     * 
     * @return the idColumn
     */
    public Column getIdColumn()
    {
        return idColumn;
    }

    /**
     * Sets the id column.
     * 
     * @param idColumn
     *            the idColumn to set
     */
    public void setIdColumn(Column idColumn)
    {
        this.idColumn = idColumn;
    }

    /**
     * Gets the read identifier method.
     * 
     * @return the readIdentifierMethod
     */
    public Method getReadIdentifierMethod()
    {
        return readIdentifierMethod;
    }

    /**
     * Sets the read identifier method.
     * 
     * @param readIdentifierMethod
     *            the readIdentifierMethod to set
     */
    public void setReadIdentifierMethod(Method readIdentifierMethod)
    {
        this.readIdentifierMethod = readIdentifierMethod;
    }

    /**
     * Gets the write identifier method.
     * 
     * @return the writeIdentifierMethod
     */
    public Method getWriteIdentifierMethod()
    {
        return writeIdentifierMethod;
    }

    /**
     * Sets the write identifier method.
     * 
     * @param writeIdentifierMethod
     *            the writeIdentifierMethod to set
     */
    public void setWriteIdentifierMethod(Method writeIdentifierMethod)
    {
        this.writeIdentifierMethod = writeIdentifierMethod;
    }

    /**
     * Gets the columns map.
     * 
     * @return the columns map
     */
    public Map<String, Column> getColumnsMap()
    {
        return columnsMap;
    }

    /**
     * Gets the super columns map.
     * 
     * @return the super columns map
     */
    public Map<String, EmbeddedColumn> getEmbeddedColumnsMap()
    {
        return embeddedColumnsMap;
    }

    /**
     * Gets the column.
     * 
     * @param key
     *            the key
     * 
     * @return the column
     */
    public Column getColumn(String key)
    {
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
    public EmbeddedColumn getEmbeddedColumn(String key)
    {
        return embeddedColumnsMap.get(key);
    }

    /**
     * Gets the columns as list.
     * 
     * @return the columns as list
     */
    public List<Column> getColumnsAsList()
    {
        return new ArrayList<Column>(columnsMap.values());
    }

    /**
     * Gets the super columns as list.
     * 
     * @return the super columns as list
     */
    public List<EmbeddedColumn> getEmbeddedColumnsAsList()
    {
        return new ArrayList<EmbeddedColumn>(embeddedColumnsMap.values());
    }

    /**
     * Gets the column field names.
     * 
     * @return the column field names
     */
    public List<String> getColumnFieldNames()
    {
        return new ArrayList<String>(columnsMap.keySet());
    }

    /**
     * Gets the super column field names.
     * 
     * @return the super column field names
     */
    public List<String> getEmbeddedColumnFieldNames()
    {
        return new ArrayList<String>(embeddedColumnsMap.keySet());
    }

    /**
     * Adds the column.
     * 
     * @param key
     *            the key
     * @param column
     *            the column
     */
    public void addColumn(String key, Column column)
    {
        columnsMap.put(key, column);
    }

    /**
     * Adds the super column.
     * 
     * @param key
     *            the key
     * @param embeddedColumn
     *            the super column
     */
    public void addEmbeddedColumn(String key, EmbeddedColumn embeddedColumn)
    {
        embeddedColumnsMap.put(key, embeddedColumn);
    }

    /**
     * Adds the index property.
     * 
     * @param index
     *            the index
     */
    public void addIndexProperty(PropertyIndex index)
    {
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
    public void addRelation(String property, Relation relation)
    {
        relationsMap.put(property, relation);
        addRelationName(relation);
    }

    /**
     * Gets the relation.
     * 
     * @param property
     *            the property
     * @return the relation
     */
    public Relation getRelation(String property)
    {
        return relationsMap.get(property);
    }

    /**
     * Gets the relations.
     * 
     * @return the relations
     */
    public List<Relation> getRelations()
    {
        return new ArrayList<Relation>(relationsMap.values());
    }

    /**
     * Gets the index properties.
     * 
     * @return the index properties
     */
    public List<PropertyIndex> getIndexProperties()
    {
        return indexPrperties;
    }

    /**
     * Gets the index name.
     * 
     * @return the index name
     */
    public String getIndexName()
    {
        return indexName;
    }

    /**
     * Sets the index name.
     * 
     * @param indexName
     *            the new index name
     */
    public void setIndexName(String indexName)
    {
        this.indexName = indexName;
    }

    /**
     * Checks if is indexable.
     * 
     * @return true, if is indexable
     */
    public boolean isIndexable()
    {
        return isIndexable;
    }

    /**
     * Sets the indexable.
     * 
     * @param isIndexable
     *            the new indexable
     */
    public void setIndexable(boolean isIndexable)
    {
        this.isIndexable = isIndexable;
    }

    /**
     * Sets the callback methods map.
     * 
     * @param callbackMethodsMap
     *            the callback methods map
     */
    public void setCallbackMethodsMap(Map<Class<?>, List<? extends CallbackMethod>> callbackMethodsMap)
    {
        this.callbackMethodsMap = callbackMethodsMap;
    }

    /**
     * Gets the callback methods map.
     * 
     * @return the callback methods map
     */
    public Map<Class<?>, List<? extends CallbackMethod>> getCallbackMethodsMap()
    {
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
    public List<? extends CallbackMethod> getCallbackMethods(Class<?> event)
    {
        return this.callbackMethodsMap.get(event);
    }

    /**
     * Checks if is embeddable.
     * 
     * @param fieldClass
     *            the field class
     * @return true, if is embeddable
     */
    public boolean isEmbeddable(Class<?> fieldClass)
    {
        return embeddableCollection.contains(fieldClass);
    }

    /**
     * Adds the to embed collection.
     * 
     * @param fieldClass
     *            the field class
     */
    public void addToEmbedCollection(Class<?> fieldClass)
    {
        if (!embeddableCollection.contains(fieldClass))
        {
            embeddableCollection.add(fieldClass);
        }
    }

    /**
     * Checks if is cacheable.
     * 
     * @return the cacheable
     */
    public boolean isCacheable()
    {
        return cacheable;
    }

    /**
     * Sets the cacheable.
     * 
     * @param cacheable
     *            the cacheable to set
     */
    public void setCacheable(boolean cacheable)
    {
        this.cacheable = cacheable;
    }
    
    

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        int start = 0;
        StringBuilder builder = new StringBuilder();
        builder.append(entityClazz.getName() + " (\n");
        builder.append("\tTable: " + tableName + ", \n");
        builder.append("\tKeyspace: " + schema + ",\n");
        builder.append("\tPersistence Unit: " + persistenceUnit + ",\n");
        builder.append("\tId: " + idColumn.getName() + ",\n");
        builder.append("\tReadIdMethod: " + readIdentifierMethod.getName() + ",\n");
        builder.append("\tWriteIdMethod: " + writeIdentifierMethod.getName() + ",\n");
        builder.append("\tCacheable: " + cacheable + ",\n");

        if (!columnsMap.isEmpty())
        {
            builder.append("\tColumns (");
            for (Column col : columnsMap.values())
            {
                if (start++ != 0)
                {
                    builder.append(", ");
                }
                builder.append(col.getName());
            }
            builder.append("),\n");
        }

        if (!embeddedColumnsMap.isEmpty())
        {
            builder.append("\tEmbedded Columns (\n");
            for (EmbeddedColumn col : embeddedColumnsMap.values())
            {
                builder.append("\t\t" + col.getName() + "(");

                start = 0;
                for (Column c : col.getColumns())
                {
                    if (start++ != 0)
                    {
                        builder.append(", ");
                    }
                    builder.append(c.getName());
                }
                builder.append(")\n");
            }
            builder.append("\t),\n");
        }

        if (!indexPrperties.isEmpty())
        {
            // builder.append("\tIndexName: " + indexName + ",\n");
            // builder.append("\tIndexBoost: " + indexBoost + ",\n");

            builder.append("\tIndexes (");
            start = 0;
            for (PropertyIndex index : indexPrperties)
            {
                if (start++ != 0)
                {
                    builder.append(", ");
                }
                builder.append(index.getName());
            }
            builder.append("),\n");
        }

        if (!callbackMethodsMap.isEmpty())
        {
            builder.append("\tListeners (\n");
            for (Map.Entry<Class<?>, List<? extends CallbackMethod>> entry : callbackMethodsMap.entrySet())
            {
                String key = entry.getKey().getSimpleName();
                for (CallbackMethod cbm : entry.getValue())
                {
                    builder.append("\t\t" + key + ": " + cbm + "\n");
                }
            }
            builder.append("\t)\n");
        }

        if (!relationsMap.isEmpty())
        {
            builder.append("\tRelation (\n");
            for (Relation rel : relationsMap.values())
            {
                builder.append("\t\t" + rel.getTargetEntity().getName() + "#" + rel.getProperty().getName());
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
     * Gets the dB type.
     * 
     * @return the dB type
     */
    public DBType getDBType()
    {
        return dbType;
    }

    /**
     * Sets the dB type.
     * 
     * @param type
     *            the new dB type
     */
    public void setDBType(DBType type)
    {
        this.dbType = type;
    }

    /**
     * Getter method for isRelatedViaJoinTable.
     * 
     * @return true, if holds join table relation, else false.
     */
    public boolean isRelationViaJoinTable()
    {
        return isRelationViaJoinTable;
    }    
    

    /**
     * @return the isParent
     */
    public boolean isParent()
    {
        return isParent;
    }

    /**
     * @param isParent the isParent to set
     */
    public void setParent(boolean isParent)
    {
        this.isParent = isParent;
    }

    /**
     * Setter method for isRelatedViaJoinTable.
     * 
     * @param isRelationViaJoinTable
     *            the new relation via join table
     */
    public void setRelationViaJoinTable(boolean isRelationViaJoinTable)
    {
        this.isRelationViaJoinTable = isRelationViaJoinTable;
    }

    public List<String> getRelationNames()
    {
        return relationNames;
    }
    /**
     * Method to add specific relation name for given relational field.
     * 
     * @param rField relation object.
     */
    private void addRelationName(Relation rField)
    {
        String relationName=getJoinColumnName(rField.getProperty());
        if(rField.getProperty().isAnnotationPresent(PrimaryKeyJoinColumn.class))
        {
            relationName = this.getIdColumn().getName();
        }
        addToRelationNameCollection(relationName);
    }

    /**
     * Adds relation name to relation name collection.
     * 
     * @param relationName relational name
     */
    private void addToRelationNameCollection(String relationName)
    {
        if(relationNames == null)
        {
            relationNames = new ArrayList<String>();
        }
        if(relationName != null)
        {
        relationNames.add(relationName);
        }
    }
    
    
    /**
     * Gets the relation field name.
     * 
     * @param relation
     *            the relation
     * @return the relation field name
     */
    private String getJoinColumnName(Field relation)
    {
        String columnName = null;
        JoinColumn ann = relation.getAnnotation(JoinColumn.class);
        if (ann != null)
        {
            columnName = ann.name();

        }
        return columnName != null ? columnName : relation.getName();
    }

}
