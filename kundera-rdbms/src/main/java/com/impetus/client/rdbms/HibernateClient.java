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
package com.impetus.client.rdbms;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.hibernate.Criteria;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Restrictions;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * The Class HibernateClient.
 * 
 * @author vivek.mishra
 */
public class HibernateClient implements Client
{

    /** The persistence unit. */
    private String persistenceUnit;

    /** The conf. */
    private Configuration conf;

    /** The sf. */
    private SessionFactory sf;

    /** The index manager. */
    private IndexManager indexManager;

    /** The s. */
    private StatelessSession s;

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new hibernate client.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     */
    public HibernateClient(final String persistenceUnit, IndexManager indexManager, EntityReader reader)
    {
        conf = new Configuration().addProperties(HibernateUtils.getProperties(persistenceUnit));
        Collection<Class<?>> classes = ((MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                persistenceUnit)).getEntityNameToClassMap().values();
        for (Class<?> c : classes)
        {
            conf.addAnnotatedClass(c);
        }
        sf = conf.buildSessionFactory();

        // TODO . once we clear this persistenceUnit stuff we need to simply
        // modify this to have a properties or even pass an EMF!
        this.persistenceUnit = persistenceUnit;
        this.indexManager = indexManager;
        this.reader = reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(com.impetus.kundera.proxy.
     * EnhancedEntity)
     */
    // TODO: This needs to be deleted.
    @Override
    public void persist(EnhancedEntity enhanceEntity) throws Exception
    {
        Session s = getSessionInstance();
        Transaction tx = s.beginTransaction();
        s.persist(enhanceEntity.getEntity());
        tx.commit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIndexManager()
     */
    @Override
    public IndexManager getIndexManager()
    {

        return indexManager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getPersistenceUnit()
     */
    @Override
    public String getPersistenceUnit()
    {

        return persistenceUnit;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#setPersistenceUnit(java.lang.String)
     */
    @Override
    @Deprecated
    public void setPersistenceUnit(String arg0)
    {
        // throw new
        // NotImplementedException("This support is already depricated");

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        this.indexManager.flush();
        if (sf != null && !sf.isClosed())
        {
            sf.close();
        }
    }

    // /*
    // * (non-Javadoc)
    // *
    // * @see
    // com.impetus.kundera.client.Client#delete(com.impetus.kundera.proxy.
    // * EnhancedEntity)
    // */
    // @Override
    // public void delete(EnhancedEntity arg0) throws Exception
    // {
    //
    // }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void delete(Object entity, Object pKey, EntityMetadata metadata) throws Exception
    {
        Session s = getSessionInstance();
        Transaction tx = s.beginTransaction();
        s.delete(entity);
        tx.commit();
        s.close();

        if (!MetadataUtils.useSecondryIndex(getPersistenceUnit()))
        {
            getIndexManager().remove(metadata, entity, pKey.toString());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.String)
     */
    @Override
    public <E> E find(Class<E> arg0, Object key, List<String> relationNames) throws Exception
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), arg0);
        Session s = getSessionInstance();
        Transaction tx = s.beginTransaction();
        E object = (E) s.get(arg0, getKey(key, entityMetadata.getIdColumn().getField()));
        tx.commit();

        return object;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.String[])
     */
    @Override
    public <E> List<E> findAll(Class<E> arg0, Object... arg1) throws Exception
    {
        // TODO: Vivek correct it. unfortunately i need to open a new session
        // for each finder to avoid lazy loading.
        List<E> objs = new ArrayList<E>();
        Session s = sf.openSession();
        Transaction tx = s.beginTransaction();

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), arg0);

        Object[] pKeys = getDataType(entityMetadata, arg1);
        String id = entityMetadata.getIdColumn().getField().getName();

        Criteria c = s.createCriteria(arg0);

        c.add(Restrictions.in(id, pKeys));

        return c.list();
    }

    /**
     * Gets the data type.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param arg1
     *            the arg1
     * @return the data type
     * @throws PropertyAccessException
     *             the property access exception
     */
    private Object[] getDataType(EntityMetadata entityMetadata, Object... arg1) throws PropertyAccessException
    {
        Field idField = entityMetadata.getIdColumn().getField();
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(idField);

        Object[] pKeys = new Object[arg1.length];
        int cnt = 0;
        for (Object r : arg1)
        {
            pKeys[cnt++] = accessor.fromString(r.toString());
        }

        return pKeys;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    @Deprecated
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) throws Exception
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persist(com.impetus.kundera.persistence
     * .handler.impl.EntitySaveGraph,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public String persist(EntitySaveGraph entityGraph, EntityMetadata metadata)
    {

        Session s;
        Transaction tx;
        try
        {
            s = getSessionInstance();
            tx = s.beginTransaction();
            s.persist(entityGraph.getParentEntity());
            tx.commit();
        }
        catch (org.hibernate.exception.ConstraintViolationException e)
        {
            e.printStackTrace();
        }

        // If entity has a parent entity, update foreign key
        if (entityGraph.getRevFKeyName() != null)
        {
            s = getSessionInstance();
            tx = s.beginTransaction();
            String updateSql = "Update " + metadata.getTableName() + " SET " + entityGraph.getRevFKeyName() + "= '"
                    + entityGraph.getRevFKeyValue() + "' WHERE " + metadata.getIdColumn().getName() + " = '"
                    + entityGraph.getParentId() + "'";
            s.createSQLQuery(updateSql).executeUpdate();
            tx.commit();
        }

        if (!MetadataUtils.useSecondryIndex(getPersistenceUnit()))
        {
            if (entityGraph.getRevParentClass() != null)
            {
                getIndexManager().write(metadata, entityGraph.getParentEntity(), entityGraph.getRevFKeyValue(),
                        entityGraph.getRevParentClass());
            }
            else
            {
                getIndexManager().write(metadata, entityGraph.getParentEntity());
            }
        }

        return null;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(java.lang.Object,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata)
    {
        // String rlName = entitySaveGraph.getfKeyName();
        String rlValue = entitySaveGraph.getParentId();
        Session s = getSessionInstance();
        Transaction tx = s.beginTransaction();
        s.persist(childEntity);
        tx.commit();

        // Update foreign key value
        if (entitySaveGraph.getfKeyName() != null)
        {
            s = getSessionInstance();
            tx = s.beginTransaction();
            String updateSql = "Update " + metadata.getTableName() + " SET " + entitySaveGraph.getfKeyName() + "= '"
                    + entitySaveGraph.getParentId() + "' WHERE " + metadata.getIdColumn().getName() + " = '"
                    + entitySaveGraph.getChildId() + "'";
            s.createSQLQuery(updateSql).executeUpdate();
            tx.commit();
        }

        onIndex(childEntity, entitySaveGraph, metadata, rlValue);
    }

    /**
     * Inserts records into JoinTable for the given relationship.
     * 
     * @param joinTableName
     *            the join table name
     * @param joinColumnName
     *            the join column name
     * @param inverseJoinColumnName
     *            the inverse join column name
     * @param relMetadata
     *            the rel metadata
     */
    @Override
    public void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata,  Object primaryKey, Object childEntity)
    {

        String parentId = (String)primaryKey;
        if (Collection.class.isAssignableFrom(childEntity.getClass()))
        {
            Collection children = (Collection) childEntity;

            for (Object child : children)
            {
                insertRecordInJoinTable(joinTableName, joinColumnName, inverseJoinColumnName, relMetadata, parentId,
                        child);
            }

        }
        else
        {            
            insertRecordInJoinTable(joinTableName, joinColumnName, inverseJoinColumnName, relMetadata, parentId, childEntity);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#getForeignKeysFromJoinTable(java.lang
     * .String, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String joinColumnName,
            String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {
        String parentId = objectGraph.getParentId();

        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append("SELECT ").append(inverseJoinColumnName).append(" FROM ").append(joinTableName)
                .append(" WHERE ").append(joinColumnName).append("='").append(parentId).append("'");

        Session s = sf.openSession();
        Transaction tx = s.beginTransaction();

        SQLQuery query = s.createSQLQuery(sqlQuery.toString());

        List<E> foreignKeys = new ArrayList<E>();

        foreignKeys = query.list();

        s.close();

        return foreignKeys;
    }

    /**
     * Insert record in join table.
     * 
     * @param joinTableName
     *            the join table name
     * @param joinColumnName
     *            the join column name
     * @param inverseJoinColumnName
     *            the inverse join column name
     * @param relMetadata
     *            the rel metadata
     * @param parentId
     *            the parent id
     * @param child
     *            the child
     */
    private void insertRecordInJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata, String parentId, Object child)
    {
        String childId = null;
        try
        {
            childId = PropertyAccessorHelper.getId(child, relMetadata);
        }
        catch (PropertyAccessException e)
        {
            e.printStackTrace();
            return;
        }

        StringBuffer query = new StringBuffer();
        query.append("INSERT INTO ").append(joinTableName).append("(").append(joinColumnName).append(",")
                .append(inverseJoinColumnName).append(")").append(" VALUES('").append(parentId).append("','")
                .append(childId).append("')");

        Session s = getSessionInstance();
        Transaction tx = s.beginTransaction();
        s.createSQLQuery(query.toString()).executeUpdate();
        tx.commit();

    }

    /**
     * On index.
     * 
     * @param childEntity
     *            the child entity
     * @param entitySaveGraph
     *            the entity save graph
     * @param metadata
     *            the metadata
     * @param rlValue
     *            the rl value
     */
    private void onIndex(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata, String rlValue)
    {
        if (!MetadataUtils.useSecondryIndex(getPersistenceUnit()))
        {
            if (!entitySaveGraph.isSharedPrimaryKey())
            {
                getIndexManager().write(metadata, childEntity, rlValue, entitySaveGraph.getParentClass());
            }
            else
            {
                getIndexManager().write(metadata, childEntity);
            }
        }
    }

    /**
     * Gets the session instance.
     * 
     * @return the session instance
     */
    private Session getSessionInstance()
    {
        Session s = null;
        if (sf.isClosed())
        {
            s = sf.openSession();
        }
        else
        {
            s = sf.getCurrentSession();
        }
        return s;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)
     */
    @Override
    public Object find(Class<?> clazz, EntityMetadata metadata, Object rowId, List<String> relations)
    {
        if (s == null)
        {
            s = sf.openStatelessSession();

            s.beginTransaction();
        }
        Object result = s.get(clazz, getKey(rowId, metadata.getIdColumn().getField()));
        // s.close();
        return result;
    }

    /**
     * Find.
     * 
     * @param nativeQuery
     *            the native query
     * @param relations
     *            the relations
     * @param clazz
     *            the clazz
     * @return the list
     */
    public List find(String nativeQuery, List<String> relations, Class clazz)
    {
        // Session s = getSessionInstance();
        List<Object[]> result = new ArrayList<Object[]>();
        if (s == null)
        {
            s = sf.openStatelessSession();

            s.beginTransaction();
        }
        SQLQuery q = s.createSQLQuery(nativeQuery).addEntity(clazz);
        for (String r : relations)
        {
            q.addScalar(r);
        }

        return q.list();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    public List<Object> find(String colName, String colValue, EntityMetadata m)
    {
        String tableName = m.getTableName();
        String aliasName = "_" + tableName;
        StringBuilder queryBuilder = new StringBuilder("Select ");
        queryBuilder.append(aliasName);
        queryBuilder.append(".*");
        queryBuilder.append("From ");
        queryBuilder.append(tableName);
        queryBuilder.append(" ");
        queryBuilder.append(aliasName);
        queryBuilder.append(" Where ");
        queryBuilder.append(colName);
        queryBuilder.append(" = ");
        queryBuilder.append(colValue);
        Session s = getSessionInstance();
        s.beginTransaction();
        SQLQuery q = s.createSQLQuery(queryBuilder.toString()).addEntity(m.getEntityClazz());
        return q.list();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    public EntityReader getReader()
    {
        return reader;
    }

    private Serializable getKey(Object pKey, Field f)
    {
        if (pKey != null)
        {
            if (f.getType().isAssignableFrom(long.class) || f.getType().isAssignableFrom(Long.class))
            {
                return Long.valueOf(pKey.toString());
            }
            else if (f.getType().isAssignableFrom(int.class) || f.getType().isAssignableFrom(Integer.class))
            {
                return Integer.valueOf(pKey.toString());
            }
            else if (f.getType().isAssignableFrom(String.class))
            {
                return (String) pKey;
            }
            else if (f.getType().isAssignableFrom(boolean.class) || f.getType().isAssignableFrom(Boolean.class))
            {
                return Boolean.valueOf(pKey.toString());
            }
            else if (f.getType().isAssignableFrom(double.class) || f.getType().isAssignableFrom(Double.class))
            {
                return Double.valueOf(pKey.toString());
            }
            else if (f.getType().isAssignableFrom(float.class) || f.getType().isAssignableFrom(Float.class))
            {
                return Float.valueOf(pKey.toString());
            }
            else
            {
                throw new PersistenceException("Unsupported type:" + pKey.getClass());
            }
        }

        return null;
    }
}
