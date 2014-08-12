/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
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
    private Class<?> entityClazz;

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

    private boolean isCounterColumnType = false;

    private SingularAttribute idAttribute;

    private Map<String, String> jpaColumnMapping = new HashMap<String, String>();

    /** The read identifier method. */
    private Method readIdentifierMethod;

    /** The write identifier method. */
    private Method writeIdentifierMethod;

    /** The index prperties. */
    private Map<String, PropertyIndex> indexPrperties = new HashMap<String, PropertyIndex>();

    /** The callback methods map. */
    private Map<Class<?>, List<? extends CallbackMethod>> callbackMethodsMap = new HashMap<Class<?>, List<? extends CallbackMethod>>();

    /** Relationship map, key=>property name, value=>relation. */
    private Map<String, Relation> relationsMap = new HashMap<String, Relation>();

    /** type. */
    private Type type;

    /** The is relation via join table. */
    private boolean isRelationViaJoinTable;

    private List<String> relationNames;

    // Whether it contains One-To-Many relationship
    private boolean isParent;

    private EntityType entityType;

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
        getEntityType();

        return this.entityType != null && !StringUtils.isBlank(((AbstractManagedType) this.entityType).getTableName()) ? ((AbstractManagedType) this.entityType)
                .getTableName() : tableName;
    }

    private EntityType getEntityType()
    {
        /*
         * if (this.entityType == null) { MetamodelImpl metaModel =
         * (MetamodelImpl)
         * kunderaMetadata.getApplicationMetadata().getMetamodel(
         * getPersistenceUnit()); if (metaModel != null) { this.entityType =
         * metaModel.entity(this.entityClazz); } }
         */
        return this.entityType;
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
        getEntityType();

        return this.entityType != null && !StringUtils.isBlank(((AbstractManagedType) this.entityType).getSchemaName()) ? ((AbstractManagedType) this.entityType)
                .getSchemaName() : schema;
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
     * Adds the index property.
     * 
     * @param index
     *            the index
     */
    public void addIndexProperty(String columnName, PropertyIndex index)
    {
        indexPrperties.put(columnName, index);

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
    public Map<String, PropertyIndex> getIndexProperties()
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
        builder.append("\tId: " + idAttribute.getName() + ",\n");
        builder.append("\tCacheable: " + cacheable + ",\n");

        if (!indexPrperties.isEmpty())
        {

            builder.append("\tIndexes (");
            start = 0;
            for (String indexColumnName : indexPrperties.keySet())
            {
                if (start++ != 0)
                {
                    builder.append(", ");
                }
                builder.append(indexPrperties.get(indexColumnName));
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
                if (rel.getMapKeyJoinClass() != null)
                    builder.append(" --- via ").append(rel.getMapKeyJoinClass().getSimpleName()).append(" ---\n");
                builder.append("\t\t" + rel.getTargetEntity().getName() + "#" + rel.getProperty().getName());
                builder.append(" (" + rel.getCascades());
                builder.append(", " + rel.getType());
                builder.append(", " + rel.getFetchType());
                builder.append(")\n");
            }
            builder.append("\t)\n");
        }

        builder.append(")");
        return builder.toString();
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
     * @param isParent
     *            the isParent to set
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
     * @param rField
     *            relation object.
     */
    private void addRelationName(Relation rField)
    {
        if (rField != null && !rField.isRelatedViaJoinTable())
        {
            String relationName = getJoinColumnName(rField.getProperty());
            if (rField.getProperty().isAnnotationPresent(PrimaryKeyJoinColumn.class))
            {
                relationName = this.getIdAttribute().getName();
            }

            addToRelationNameCollection(relationName);
        }
    }

    /**
     * Adds relation name to relation name collection.
     * 
     * @param relationName
     *            relational name
     */
    private void addToRelationNameCollection(String relationName)
    {
        if (relationNames == null)
        {
            relationNames = new ArrayList<String>();
        }
        if (relationName != null)
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
        return StringUtils.isBlank(columnName) ? relation.getName() : columnName;
    }

    /**
     * @return the isCounterColumnType
     */
    public boolean isCounterColumnType()
    {
        return isCounterColumnType;
    }

    /**
     * @param isCounterColumnType
     *            the isCounterColumnType to set
     */
    public void setCounterColumnType(boolean isCounterColumnType)
    {
        this.isCounterColumnType = isCounterColumnType;
    }

    /**
     * @return the idAttribute
     */
    public SingularAttribute getIdAttribute()
    {
        return idAttribute;
    }

    /**
     * @param idAttribute
     *            the idAttribute to set
     */
    public void setIdAttribute(SingularAttribute idAttribute)
    {
        this.idAttribute = idAttribute;
    }

    public void addJPAColumnMapping(String jpaColumnName, String fieldName)
    {
        jpaColumnMapping.put(jpaColumnName, fieldName);
    }

    public String getFieldName(String jpaColumnName)
    {
        // if(jpaColumnName.equals(((AbstractAttribute)this.getIdAttribute()).getJPAColumnName()))
        // {
        // return this.getIdAttribute().getName();
        // }

        String fieldName = jpaColumnMapping.get(jpaColumnName);

        if (fieldName == null)
        {
            getEntityType();
            MetadataUtils.onJPAColumnMapping(this.entityType, this); // rebase.
                                                                     // require
                                                                     // in case
                                                                     // of
                                                                     // concrete
                                                                     // super
                                                                     // entity
                                                                     // class.
            fieldName = jpaColumnMapping.get(jpaColumnName);
        }

        if (fieldName == null && jpaColumnName.equals(((AbstractAttribute) this.getIdAttribute()).getJPAColumnName()))
        {
            return this.getIdAttribute().getName();
        }

        return fieldName;
    }

    public void setEntityType(EntityType entityType)
    {
        if (entityType != null)
        {
            this.entityType = entityType;
        }
    }

}
