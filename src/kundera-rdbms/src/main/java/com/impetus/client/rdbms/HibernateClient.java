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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.GenerationType;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Parameter;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.lang.StringUtils;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.hibernate.internal.StatelessSessionImpl;
import org.hibernate.transform.AliasToEntityMapResultTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.query.RDBMSQuery;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class HibernateClient.
 * 
 * @author vivek.mishra
 */
public class HibernateClient extends ClientBase implements Client<RDBMSQuery>
{

    /** The client factory. */
    private RDBMSClientFactory clientFactory;

    /** The s. */
    private StatelessSession s;

    /** The reader. */
    private EntityReader reader;

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(HibernateClient.class);

    /**
     * Instantiates a new hibernate client.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     * @param clientFactory
     *            the client factory
     * @param externalProperties
     *            the external properties
     * @param clientMetadata
     *            the client metadata
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public HibernateClient(final String persistenceUnit, IndexManager indexManager, EntityReader reader,
            RDBMSClientFactory clientFactory, Map<String, Object> externalProperties,
            final ClientMetadata clientMetadata, final KunderaMetadata kunderaMetadata)
    {

        super(kunderaMetadata, externalProperties, persistenceUnit);
        this.clientFactory = clientFactory;
        // TODO . once we clear this persistenceUnit stuff we need to simply
        // modify this to have a properties or even pass an EMF!
        this.indexManager = indexManager;
        this.reader = reader;
        this.clientMetadata = clientMetadata;
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
        externalProperties = null;
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
        s = getStatelessSession();
        Transaction tx = null;
        tx = onBegin();
        s.delete(entity);
        onCommit(tx);

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(kunderaMetadata,
                metadata.getPersistenceUnit());
        if (!MetadataUtils.useSecondryIndex(getClientMetadata()))
        {
            getIndexManager().remove(metadata, entity, pKey);
        }
    }

    /**
     * On begin.
     * 
     * @return the transaction
     */
    private Transaction onBegin()
    {
        Transaction tx;
        if (((StatelessSessionImpl) s).getTransactionCoordinator().isTransactionActive())
        {
            tx = ((StatelessSessionImpl) s).getTransaction();
        }
        else
        {
            tx = s.beginTransaction();
        }
        return tx;
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
        s = getStatelessSession();

        Object result = null;
        try
        {
            result = s.get(clazz, (Serializable) key);
        }
        catch (ClassCastException ccex)
        {
            log.error("Class can not be serializable, Caused by {}.", ccex);
            throw new KunderaException(ccex);
        }
        catch (Exception e)
        {
            log.error("Error while finding, Caused by {}. ", e);
            throw new KunderaException(e);
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
    public <E> List<E> findAll(Class<E> entityClazz, String[] columnsToSelect, Object... arg1)
    {
        // TODO: Vivek correct it. unfortunately i need to open a new session
        // for each finder to avoid lazy loading.
        Session s = getSession();

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, getPersistenceUnit(),
                entityClazz);

        Object[] pKeys = getDataType(entityMetadata, arg1);
        String id = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();

        Criteria c = s.createCriteria(entityClazz);

        c.add(Restrictions.in(id, pKeys));

        return c.list();
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
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata
     * .model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata metadata, Object entity, Object id, List<RelationHolder> relationHolders)
    {
        boolean proxyRemoved = removeKunderaProxies(metadata, entity, relationHolders);

        Transaction tx = null;

        s = getStatelessSession();
        tx = onBegin();
        try
        {
            if (!isUpdate)
            {
                id = s.insert(entity);

                // Update foreign Keys
                updateForeignKeys(metadata, id, relationHolders);
                onCommit(tx);/* tx.commit(); */
            }
            else
            {
                s.update(entity);

                if (proxyRemoved)
                {
                    updateForeignKeys(metadata, id, relationHolders);
                }

                onCommit(tx);
            }
        }
        // TODO: Bad code, get rid of these exceptions, currently necessary for
        // handling many to one case
        catch (org.hibernate.exception.ConstraintViolationException e)
        {
            s.update(entity);
            log.info(e.getMessage());
            onCommit(tx);
            // tx.commit();
        }
        catch (HibernateException e)
        {
            log.error("Error while persisting object of {}, Caused by {}.", metadata.getEntityClazz(), e);
            throw new PersistenceException(e);
        }
    }

    /**
     * On commit.
     * 
     * @param tx
     *            the tx
     */
    private void onCommit(Transaction tx)
    {
        if (tx.isActive())
        {
            tx.commit();
        }
    }

    /**
     * Inserts records into JoinTable.
     * 
     * @param joinTableData
     *            the join table data
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String schemaName = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, joinTableData.getEntityClass())
                .getSchema();
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Object parentId, Class columnJavaType)
    {
        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append("SELECT ").append(inverseJoinColumnName).append(" FROM ")
                .append(getFromClause(schemaName, joinTableName)).append(" WHERE ").append(joinColumnName).append("='")
                .append(parentId).append("'");

        Session s = getSession();

        SQLQuery query = s.createSQLQuery(sqlQuery.toString());

        List<E> foreignKeys = new ArrayList<E>();

        foreignKeys = query.list();

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

        Session s = getSession();

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

        StatelessSession s = getStatelessSession();

        Transaction tx = onBegin();
        onNativeUpdate(query.toString(), null);
        onCommit(tx);

    }

    /**
     * Insert record in join table.
     * 
     * @param schemaName
     *            the schema name
     * @param joinTableName
     *            the join table name
     * @param joinColumnName
     *            the join column name
     * @param inverseJoinColumnName
     *            the inverse join column name
     * @param parentId
     *            the parent id
     * @param childrenIds
     *            the children ids
     */
    private void insertRecordInJoinTable(String schemaName, String joinTableName, String joinColumnName,
            String inverseJoinColumnName, Object parentId, Set<Object> childrenIds)
    {
        s = getStatelessSession();
        Transaction tx = onBegin();
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
        onCommit(tx);
        // tx.commit();
    }

    /**
     * Gets the session instance.
     * 
     * @return the session instance
     */
    private StatelessSession getStatelessSession()
    {
        return s != null ? s : clientFactory.getStatelessSession();
    }

    /**
     * Gets the session instance.
     * 
     * @return the session instance
     */
    private Session getSession()
    {
        return clientFactory.getSession();
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
        List entities = new ArrayList();

        s = getStatelessSession();

        SQLQuery q = s.createSQLQuery(nativeQuery);
        q.setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE);

        List result = q.list();

        try
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());

            EntityType entityType = metaModel.entity(m.getEntityClazz());

            List<AbstractManagedType> subManagedType = ((AbstractManagedType) entityType).getSubManagedType();
            for (Object o : result)
            {
                Map<String, Object> relationValue = null;
                Object entity = null;
                EntityMetadata subEntityMetadata = null;
                if (!subManagedType.isEmpty())
                {
                    for (AbstractManagedType subEntity : subManagedType)
                    {
                        String discColumn = subEntity.getDiscriminatorColumn();
                        String disColValue = subEntity.getDiscriminatorValue();
                        Object value = ((Map<String, Object>) o).get(discColumn);
                        if (value != null && value.toString().equals(disColValue))
                        {
                            subEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                                    subEntity.getJavaType());
                            break;
                        }
                    }
                    entity = instantiateEntity(subEntityMetadata.getEntityClazz(), entity);
                    relationValue = HibernateUtils.getTranslatedObject(kunderaMetadata, entity,
                            (Map<String, Object>) o, m);

                }
                else
                {
                    entity = instantiateEntity(m.getEntityClazz(), entity);
                    relationValue = HibernateUtils.getTranslatedObject(kunderaMetadata, entity,
                            (Map<String, Object>) o, m);
                }

                if (relationValue != null && !relationValue.isEmpty())
                {
                    entity = new EnhanceEntity(entity, PropertyAccessorHelper.getId(entity, m), relationValue);
                }
                entities.add(entity);
            }
            return entities;
        }
        catch (Exception e)
        {
            if (e.getMessage().equals("Can not be translated into entity."))
            {
                return result;
            }
            throw new EntityReaderException(e);
        }
    }

    /**
     * Find.
     * 
     * @param query
     *            the native fquery
     * @param parameterMap
     *            the parameter map
     * @param maxResult 
     * @param firstResult 
     * @return the list
     */
    public List findByQuery(String query, Map<Parameter, Object> parameterMap, int firstResult, int maxResult)
    {
        s = getStatelessSession();

        Query q = s.createQuery(query);
        q.setFirstResult(firstResult);
        q.setMaxResults(maxResult);

        setParameters(parameterMap, q);

        return q.list();
    }

    /**
     * On native update.
     * 
     * @param query
     *            the query
     * @param parameterMap
     *            the parameter map
     * @return the int
     */
    public int onNativeUpdate(String query, Map<Parameter, Object> parameterMap)
    {
        s = getStatelessSession();

        Query q = s.createSQLQuery(query);
        setParameters(parameterMap, q);

        // Transaction tx = s.getTransaction() == null ? s.beginTransaction():
        // s.getTransaction();

        // tx.begin();
        int i = q.executeUpdate();

        // tx.commit();

        return i;
    }

    /**
     * Find.
     * 
     * @param query
     *            the native fquery
     * @param parameterMap
     *            the parameter map
     * @param maxResult 
     * @param firstResult 
     * @return the list
     */
    public int onExecuteUpdate(String query, Map<Parameter, Object> parameterMap, int firstResult, int maxResult)
    {
        s = getStatelessSession();

        Query q = s.createQuery(query);
        
        q.setFirstResult(firstResult);
        q.setMaxResults(maxResult);
        
        setParameters(parameterMap, q);

        Transaction tx = onBegin();

        int i = q.executeUpdate();

        onCommit(tx);
        // tx.commit();

        return i;
    }

    /**
     * Gets the query instance.
     * 
     * @param nativeQuery
     *            the native query
     * @param m
     *            the m
     * @return the query instance
     */
    public SQLQuery getQueryInstance(String nativeQuery, EntityMetadata m)
    {
        s = getStatelessSession();

        SQLQuery q = s.createSQLQuery(nativeQuery).addEntity(m.getEntityClazz());

        List<String> relations = m.getRelationNames();
        if (relations != null)
        {
            for (String r : relations)
            {
                Relation rel = m.getRelation(m.getFieldName(r));
                String name = MetadataUtils.getMappedName(m, m.getRelation(r), kunderaMetadata);
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
        return q;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    public List findByRelation(String colName, Object colValue, Class entityClazz)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClazz);
        String tableName = m.getTableName();

        // Suffixing the UNDERSCORE instead of prefix as Oracle 11g complains
        // about invalid characters error while executing the request.
        StringBuilder queryBuilder = new StringBuilder("Select ");

        queryBuilder.append("* ");
        queryBuilder.append("From ");
        queryBuilder.append(getFromClause(m.getSchema(), tableName));
        queryBuilder.append(" ");

        queryBuilder.append(" Where ");
        queryBuilder.append(colName);
        queryBuilder.append(" = ");
        queryBuilder.append("'");
        queryBuilder.append(colValue);
        queryBuilder.append("'");
        s = getStatelessSession();

        List results = find(queryBuilder.toString(), m.getRelationNames(), m);
        return populateEnhanceEntities(m, m.getRelationNames(), results);
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
     * Gets the from clause.
     * 
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @return the from clause
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

    /**
     * Updates foreign keys into master table.
     * 
     * @param metadata
     *            the metadata
     * @param id
     *            the id
     * @param relationHolders
     *            the relation holders
     */
    private void updateForeignKeys(EntityMetadata metadata, Object id, List<RelationHolder> relationHolders)
    {
        for (RelationHolder rh : relationHolders)
        {
            String linkName = rh.getRelationName();
            Object linkValue = rh.getRelationValue();
            if (linkName != null && linkValue != null)
            {

                // String fieldName = metadata.getFieldName(linkName);

                String clause = getFromClause(metadata.getSchema(), metadata.getTableName());
                // String updateSql = "Update " +
                // metadata.getEntityClazz().getSimpleName() + " SET " +
                // fieldName + "= '" + linkValue + "' WHERE "
                // + ((AbstractAttribute) metadata.getIdAttribute()).getName() +
                // " = '" + id + "'";

                String updateSql = "Update " + clause + " SET " + linkName + "= '" + linkValue + "' WHERE "
                        + ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName() + " = '" + id + "'";

                onNativeUpdate(updateSql, null);
            }
        }
    }

    /**
     * Removes the kundera proxies.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * @param relationHolders
     *            the relation holders
     * @return true, if successful
     */
    private boolean removeKunderaProxies(EntityMetadata metadata, Object entity, List<RelationHolder> relationHolders)
    {
        boolean proxyRemoved = false;

        for (Relation relation : metadata.getRelations())
        {
            if (relation != null && relation.isUnary())
            {
                Object relationObject = PropertyAccessorHelper.getObject(entity, relation.getProperty());
                if (relationObject != null && ProxyHelper.isKunderaProxy(relationObject))
                {
                    EntityMetadata relMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                            relation.getTargetEntity());
                    Method idAccessorMethod = relMetadata.getReadIdentifierMethod();
                    Object foreignKey = null;
                    try
                    {
                        foreignKey = idAccessorMethod.invoke(relationObject, new Object[] {});
                    }
                    catch (IllegalArgumentException e)
                    {
                        log.error("Error while Fetching relationship value of {}, Caused by {}.",
                                metadata.getEntityClazz(), e);
                    }
                    catch (IllegalAccessException e)
                    {
                        log.error("Error while Fetching relationship value of {}, Caused by {}.",
                                metadata.getEntityClazz(), e);
                    }
                    catch (InvocationTargetException e)
                    {
                        log.error("Error while Fetching relationship value of {}, Caused by {}.",
                                metadata.getEntityClazz(), e);
                    }

                    if (foreignKey != null)
                    {
                        relationObject = null;
                        PropertyAccessorHelper.set(entity, relation.getProperty(), relationObject);
                        relationHolders
                                .add(new RelationHolder(relation.getJoinColumnName(kunderaMetadata), foreignKey));
                        proxyRemoved = true;
                    }
                }
            }
        }
        return proxyRemoved;
    }

    /**
     * Populate enhance entities.
     * 
     * @param m
     *            the m
     * @param relationNames
     *            the relation names
     * @param result
     *            the result
     * @return the list
     */
    private List<EnhanceEntity> populateEnhanceEntities(EntityMetadata m, List<String> relationNames, List result)
    {

        List<EnhanceEntity> ls = null;
        if (!result.isEmpty())
        {
            ls = new ArrayList<EnhanceEntity>(result.size());
            for (Object o : result)
            {
                EnhanceEntity entity = null;
                if (!o.getClass().isAssignableFrom(EnhanceEntity.class))
                {
                    entity = new EnhanceEntity(o, PropertyAccessorHelper.getId(o, m), null);
                }
                else
                {
                    entity = (EnhanceEntity) o;
                }
                ls.add(entity);
            }
        }
        return ls;
    }

    /**
     * Instantiate entity.
     * 
     * @param entityClass
     *            the entity class
     * @param entity
     *            the entity
     * @return the object
     */
    private Object instantiateEntity(Class entityClass, Object entity)
    {
        try
        {
            if (entity == null)
            {
                return entityClass.newInstance();
            }
            return entity;
        }
        catch (InstantiationException e)
        {
            log.error("Error while instantiating " + entityClass + ", Caused by: ", e);
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while instantiating " + entityClass + ", Caused by: ", e);
        }
        return null;
    }

    /**
     * Sets the parameters.
     * 
     * @param parameterMap
     *            the parameter map
     * @param q
     *            the q
     */
    private void setParameters(Map<Parameter, Object> parameterMap, Query q)
    {
        if (parameterMap != null && !parameterMap.isEmpty())
        {
            for (Parameter parameter : parameterMap.keySet())
            {
                Object paramObject = parameterMap.get(parameter);
                if (parameter.getName() != null)
                {
                    if (paramObject instanceof Collection)
                    {
                        q.setParameterList(parameter.getName(), (Collection) paramObject);
                    }
                    else
                    {
                        q.setParameter(parameter.getName(), paramObject);
                    }
                }
                else if (parameter.getPosition() != null)
                {
                    if (paramObject instanceof Collection)
                    {
                        q.setParameterList(Integer.toString(parameter.getPosition()), (Collection) paramObject);
                    }
                    else
                    {
                        q.setParameter(Integer.toString(parameter.getPosition()), paramObject);
                    }
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        throw new UnsupportedOperationException(GenerationType.class.getSimpleName()
                + " Strategies not supported by this client : HibernateClient");
    }

}