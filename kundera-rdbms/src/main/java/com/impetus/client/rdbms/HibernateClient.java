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
package com.impetus.client.rdbms;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import com.impetus.client.rdbms.query.RDBMSQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * The Class HibernateClient.
 * 
 * @author vivek.mishra
 */
public class HibernateClient extends ClientBase implements Client<RDBMSQuery>
{
    /** The sf. */
    private SessionFactory sf;

    /** The s. */
    private StatelessSession s;

    /** The reader. */
    private EntityReader reader;

    private Map<String, Object> puProperties;

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(HibernateClient.class);

    /**
     * Instantiates a new hibernate client.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     * @param puProperties
     */
    public HibernateClient(final String persistenceUnit, IndexManager indexManager, EntityReader reader,
            SessionFactory sf, Map<String, Object> puProperties)
    {

        this.sf = sf;
        // TODO . once we clear this persistenceUnit stuff we need to simply
        // modify this to have a properties or even pass an EMF!
        this.persistenceUnit = persistenceUnit;
        this.indexManager = indexManager;
        this.reader = reader;
        this.puProperties = puProperties;
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
        if (s != null)
        {
            s.close();
            s = null;
        }
        puProperties = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        s = getSessionFactory().openStatelessSession();
        Transaction tx = s.beginTransaction();
        s.delete(entity);
        tx.commit();

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
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
    public Object find(Class clazz, Object key)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), clazz);

        if (s == null)
        {
            s = getSessionFactory().openStatelessSession();

            s.beginTransaction();
        }

        Object result = null;
        try
        {
            result = s.get(clazz, getKey(key, (Field) entityMetadata.getIdAttribute().getJavaMember()));
        }
        catch (Exception e)
        {
            log.info(e.getMessage());
        }
        return result;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.String[])
     */
    @Override
    public <E> List<E> findAll(Class<E> arg0, Object... arg1)
    {
        // TODO: Vivek correct it. unfortunately i need to open a new session
        // for each finder to avoid lazy loading.
        List<E> objs = new ArrayList<E>();
        Session s = getSessionFactory().openSession();
        Transaction tx = s.beginTransaction();

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), arg0);

        Object[] pKeys = getDataType(entityMetadata, arg1);
        String id = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();

        Criteria c = s.createCriteria(arg0);

        c.add(Restrictions.in(id, pKeys));

        return c.list();
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    @Override
    protected void onPersist(EntityMetadata metadata, Object entity, Object id, List<RelationHolder> relationHolders)
    {
        Transaction tx = null;

        s = getSessionFactory().openStatelessSession();
        tx = s.beginTransaction();
        try
        {
            if (!isUpdate)
            {
                id = s.insert(entity);

                // Update foreign Keys
                for (RelationHolder rh : relationHolders)
                {
                    String linkName = rh.getRelationName();
                    Object linkValue = rh.getRelationValue();
                    if (linkName != null && linkValue != null)
                    {

                        String clause = getFromClause(metadata.getSchema(), metadata.getTableName());
                        String updateSql = "Update " + clause + " SET " + linkName + "= '" + linkValue + "' WHERE "
                                + ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName() + " = '" + id
                                + "'";
                        s.createSQLQuery(updateSql).executeUpdate();
                    }
                }
                tx.commit();
            }
            else
            {
                s.update(entity);
                tx.commit();
            }
        }
        // TODO: Bad code, get rid of these exceptions, currently necessary for
        // handling many to one case
        catch (org.hibernate.exception.ConstraintViolationException e)
        {
            s.update(entity);
            log.info(e.getMessage());
            tx.commit();
        }
        catch (HibernateException e)
        {
            log.error(e);
            e.printStackTrace();
            throw new PersistenceException(e);
        }
        finally
        {
        }
    }

    /**
     * Inserts records into JoinTable
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String schemaName = KunderaMetadataManager.getEntityMetadata(joinTableData.getEntityClass()).getSchema();
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();

        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        for (Object key : joinTableRecords.keySet())
        {
            Set<Object> values = joinTableRecords.get(key);
            insertRecordInJoinTable(schemaName, joinTableName, joinColumnName, invJoinColumnName, key, values);
        }
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Object parentId)
    {

        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append("SELECT ").append(inverseJoinColumnName).append(" FROM ")
                .append(getFromClause(schemaName, joinTableName)).append(" WHERE ").append(joinColumnName).append("='")
                .append(parentId).append("'");

        Session s = getSessionFactory().openSession();
        Transaction tx = s.beginTransaction();

        SQLQuery query = s.createSQLQuery(sqlQuery.toString());

        List<E> foreignKeys = new ArrayList<E>();

        foreignKeys = query.list();

        tx.commit();

        return foreignKeys;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        String childIdStr = (String) columnValue;
        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append("SELECT ").append(pKeyName).append(" FROM ").append(getFromClause(schemaName, tableName))
                .append(" WHERE ").append(columnName).append("='").append(childIdStr).append("'");

        Session s = getSessionFactory().openSession();
        // Transaction tx = s.beginTransaction();

        SQLQuery query = s.createSQLQuery(sqlQuery.toString());

        List<Object> primaryKeys = new ArrayList<Object>();

        primaryKeys = query.list();

        if (primaryKeys != null && !primaryKeys.isEmpty())
        {
            return primaryKeys.toArray(new Object[0]);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.Object)
     */
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {

        StringBuffer query = new StringBuffer();

        query.append("DELETE FROM ").append(getFromClause(schemaName, tableName)).append(" WHERE ").append(columnName)
                .append("=").append("'").append(columnValue).append("'");

        s = getStatelessSession();
        Transaction tx = s.beginTransaction();
        s.createSQLQuery(query.toString()).executeUpdate();
        tx.commit();
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
     * @param schema
     * @param child
     *            the child
     */
    private void insertRecordInJoinTable(String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Object parentId, Set<Object> childrenIds)
    {
        s = getSessionFactory().openStatelessSession();
        Transaction tx = s.beginTransaction();
        for (Object childId : childrenIds)
        {
            StringBuffer query = new StringBuffer();

            // write an update query
            Object[] existingRowIds = findIdsByColumn(schemaName, joinTableName, joinColumnName, inverseJoinColumnName,
                    (String) childId, null);

            boolean joinTableRecordsExists = false;
            if (existingRowIds != null && existingRowIds.length > 0)
            {
                for (Object o : existingRowIds)
                {
                    if (o.toString().equals(parentId.toString()))
                    {
                        joinTableRecordsExists = true;
                        break;
                    }
                }
            }

            if (!joinTableRecordsExists)
            {
                query.append("INSERT INTO ").append(getFromClause(schemaName, joinTableName)).append("(")
                        .append(joinColumnName).append(",").append(inverseJoinColumnName).append(")")
                        .append(" VALUES('").append(parentId).append("','").append(childId).append("')");

                s.createSQLQuery(query.toString()).executeUpdate();
            }
        }
        tx.commit();
    }

    /**
     * Gets the session instance.
     * 
     * @return the session instance
     */
    private StatelessSession getStatelessSession()
    {
        return s != null ? s : getSessionFactory().openStatelessSession();
    }

    /**
     * Find.
     * 
     * @param nativeQuery
     *            the native fquery
     * @param relations
     *            the relations
     * @param m
     *            the m
     * @return the list
     */
    public List find(String nativeQuery, List<String> relations, EntityMetadata m)
    {
        List<Object[]> result = new ArrayList<Object[]>();

        s = getSessionFactory().openStatelessSession();

        s.beginTransaction();
        SQLQuery q = s.createSQLQuery(nativeQuery).addEntity(m.getEntityClazz());
        if (relations != null)
        {
            for (String r : relations)
            {
                Relation rel = m.getRelation(m.getFieldName(r));
                String name = MetadataUtils.getMappedName(m, m.getRelation(r));
                if (!((AbstractAttribute) m.getIdAttribute()).getJPAColumnName().equalsIgnoreCase(
                        name != null ? name : r)
                        && rel != null
                        && !rel.getProperty().isAnnotationPresent(ManyToMany.class)
                        && !rel.getProperty().isAnnotationPresent(OneToMany.class)
                        && (rel.getProperty().isAnnotationPresent(OneToOne.class)
                                && StringUtils.isBlank(rel.getMappedBy()) || rel.getProperty().isAnnotationPresent(
                                ManyToOne.class)))
                {
                    q.addScalar(name != null ? name : r);
                }
            }
        }

        s.getTransaction().commit();
        return q.list();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClazz);
        String tableName = m.getTableName();
        String aliasName = "_" + tableName;
        StringBuilder queryBuilder = new StringBuilder("Select ");
        queryBuilder.append(aliasName);
        queryBuilder.append(".* ");
        queryBuilder.append("From ");
        queryBuilder.append(getFromClause(m.getSchema(), tableName));
        queryBuilder.append(" ");
        queryBuilder.append(aliasName);
        queryBuilder.append(" Where ");
        queryBuilder.append(colName);
        queryBuilder.append(" = ");
        queryBuilder.append("'");
        queryBuilder.append(colValue);
        queryBuilder.append("'");
        s = getSessionFactory().openStatelessSession();
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<RDBMSQuery> getQueryImplementor()
    {
        return RDBMSQuery.class;
    }

    /**
     * Gets the key.
     * 
     * @param pKey
     *            the key
     * @param f
     *            the f
     * @return the key
     */
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
        Field idField = (Field) entityMetadata.getIdAttribute().getJavaMember();
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(idField);

        Object[] pKeys = new Object[arg1.length];
        int cnt = 0;
        for (Object r : arg1)
        {
            pKeys[cnt++] = accessor.fromString(idField.getClass(), r.toString());
        }

        return pKeys;
    }

    /**
     * @param metadata
     * @return
     */
    private String getFromClause(String schemaName, String tableName)
    {
        String clause = tableName;
        if (schemaName != null && !schemaName.isEmpty())
        {
            clause = schemaName + "." + tableName;
        }
        return clause;
    }

    private SessionFactory getSessionFactory()
    {
        return sf;
    }
}
