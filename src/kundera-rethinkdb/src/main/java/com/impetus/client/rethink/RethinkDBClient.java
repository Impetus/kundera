/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.client.rethink;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Id;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import com.impetus.client.rethink.query.RethinkDBQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;

/**
 * The Class RethinkDBClient.
 * 
 * @author karthikp.manchala
 */
public class RethinkDBClient extends ClientBase implements Client<RethinkDBQuery>, ClientPropertiesSetter
{

    /** The Constant ID. */
    private static final String ID = "id";

    /** The connection. */
    private Connection connection;

    /** The Constant r. */
    private static final RethinkDB r = RethinkDB.r;

    /**
     * Gets the connection.
     * 
     * @return the connection
     */
    public Connection getConnection()
    {
        return connection;
    }

    /**
     * Gets the r.
     * 
     * @return the r
     */
    public static RethinkDB getR()
    {
        return r;
    }

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new rethink db client.
     * 
     * @param kunderaMetadata
     *            the kundera metadata
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     * @param properties
     *            the properties
     * @param persistenceUnit
     *            the persistence unit
     * @param connection
     *            the connection
     * @param clientMetadata
     *            the client metadata
     */
    protected RethinkDBClient(KunderaMetadata kunderaMetadata, IndexManager indexManager, EntityReader reader,
            Map<String, Object> properties, String persistenceUnit, Connection connection,
            ClientMetadata clientMetadata)
    {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.connection = connection;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientPropertiesSetter#
     * populateClientProperties(com.impetus.kundera.client.Client,
     * java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public Object find(Class entityClass, Object key)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);

        Object e = r.db(entityMetadata.getSchema()).table(entityMetadata.getTableName()).get(key).run(connection,
                entityClass);

        PropertyAccessorHelper.setId(e, entityMetadata, key);

        return e;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.String[], java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera.
     * persistence.context.jointable.JoinTableData )
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<RethinkDBQuery> getQueryImplementor()
    {
        // TODO Auto-generated method stub
        return RethinkDBQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.
     * metadata.model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        if (!isUpdate)
        {
            r.db(entityMetadata.getSchema()).table(entityMetadata.getTableName())
                    .insert(populateRmap(entityMetadata, entity)).run(connection);
        }
        else
        {
            r.db(entityMetadata.getSchema()).table(entityMetadata.getTableName())
                    .update(populateRmap(entityMetadata, entity)).run(connection);
        }
    }

    /**
     * Populate rmap.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @return the map object
     */
    private MapObject populateRmap(EntityMetadata entityMetadata, Object entity)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());
        Class entityClazz = entityMetadata.getEntityClazz();
        EntityType entityType = metaModel.entity(entityClazz);
        Set<Attribute> attributes = entityType.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        return iterateAndPopulateRmap(entity, metaModel, iterator);
    }

    /**
     * Iterate and populate rmap.
     * 
     * @param entity
     *            the entity
     * @param metaModel
     *            the meta model
     * @param iterator
     *            the iterator
     * @return the map object
     */
    private MapObject iterateAndPopulateRmap(Object entity, MetamodelImpl metaModel, Iterator<Attribute> iterator)
    {
        MapObject map = r.hashMap();
        while (iterator.hasNext())
        {
            Attribute attribute = iterator.next();
            Field field = (Field) attribute.getJavaMember();
            Object value = PropertyAccessorHelper.getObject(entity, field);
            if (field.isAnnotationPresent(Id.class))
            {
                map.with(ID, value);
            }
            else
            {
                map.with(((AbstractAttribute) attribute).getJPAColumnName(), value);
            }
        }
        return map;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    protected void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        r.db(entityMetadata.getSchema()).table(entityMetadata.getTableName()).get(pKey).delete().run(connection);
    }

}
