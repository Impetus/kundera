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
package com.impetus.client.mongodb.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.StringUtils;
import org.eclipse.persistence.jpa.jpql.parser.CountFunction;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.mongodb.MongoEntityReader;
import com.impetus.client.mongodb.query.gis.GeospatialQueryFactory;
import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.gis.query.GeospatialQuery;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.query.JPQLParseException;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.SortOrder;
import com.impetus.kundera.query.KunderaQuery.SortOrdering;
import com.impetus.kundera.query.KunderaQuery.UpdateClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;
import com.mongodb.BasicDBObject;

/**
 * Query class for MongoDB data store.
 * 
 * @author amresh.singh
 */
public class MongoDBQuery extends QueryImpl
{
    /** The log used by this class. */
    private static Logger log = LoggerFactory.getLogger(MongoDBQuery.class);

    /** The is single result. */
    private boolean isSingleResult;

    /**
     * Instantiates a new mongo db query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public MongoDBQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#executeUpdate()
     */
    @Override
    public int executeUpdate()
    {
        return super.executeUpdate();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#setFirstResult(int)
     */
    @Override
    public Query setFirstResult(int firstResult)
    {
        return super.setFirstResult(firstResult);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#setMaxResults(int)
     */
    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        List<Object> result = new ArrayList<Object>();

        try
        {
            String query = appMetadata.getQuery(getJPAQuery());

            boolean isNative = kunderaQuery.isNative();

            if (isNative)
            { // Native Query Support is enabled
                return ((MongoDBClient) client).executeQuery(query == null ? getJPAQuery() : query, m);
            }

            if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
            {
                BasicDBObject orderByClause = getOrderByClause(m);
                return ((MongoDBClient) client).loadData(m,
                        createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), null, orderByClause,
                        isSingleResult ? 1 : maxResult, firstResult, isCountQuery(),
                        getKeys(m, getKunderaQuery().getResult()), getKunderaQuery().getResult());
            }
            else
            {
                return populateUsingLucene(m, client, null, getKunderaQuery().getResult());
            }

        }
        catch (Exception e)
        {

            log.error("Error during executing query, Caused by:", e);
            throw new QueryHandlerException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#findUsingLucene(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {
        try
        {
            BasicDBObject orderByClause = getOrderByClause(m);
            // find on id, so no need to add skip() [firstResult hardcoded 0]
            return ((MongoDBClient) client).loadData(m, createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()),
                    null, orderByClause, isSingleResult ? 1 : maxResult, 0, isCountQuery(),
                    getKeys(m, getKunderaQuery().getResult()), getKunderaQuery().getResult());
        }
        catch (Exception e)
        {
            log.error("Error during executing query, Caused by:", e);
            throw new QueryHandlerException(e);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        // TODO : required to modify client return relation.
        // if it is a parent..then find data related to it only
        // else u need to load for associated fields too.
        List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        try
        {
            String query = appMetadata.getQuery(getJPAQuery());
            boolean isNative = kunderaQuery.isNative();

            if (isNative)
            {
                return ((MongoDBClient) client).executeQuery(query == null ? getJPAQuery() : query, m);

            }
            if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
            {
                BasicDBObject orderByClause = getOrderByClause(m);
                ls = ((MongoDBClient) client).loadData(m,
                        createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), m.getRelationNames(),
                        orderByClause, isSingleResult ? 1 : maxResult, firstResult, isCountQuery(),
                        getKeys(m, getKunderaQuery().getResult()), getKunderaQuery().getResult());
            }
            else
            {
                return populateUsingLucene(m, client, null, getKunderaQuery().getResult());
            }
        }
        catch (Exception e)
        {
            log.error("Error during executing query, Caused by:", e);
            throw new QueryHandlerException(e);
        }

        return setRelationEntities(ls, client, m);
    }

    /**
     * Checks if is count query.
     * 
     * @return true, if is count query
     */
    private boolean isCountQuery()
    {
        if (getKunderaQuery().getSelectStatement() != null)
        {
            final Expression selectClause = getKunderaQuery().getSelectStatement().getSelectClause();

            if (selectClause instanceof SelectClause)
            {
                final Expression expression = ((SelectClause) selectClause).getSelectExpression();

                return expression instanceof CountFunction;
            }
        }

        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return new MongoEntityReader(kunderaQuery, kunderaMetadata);
    }

    /**
     * The Class QueryComponent.
     */
    static class QueryComponent
    {

        /** The is and. */
        boolean isAnd;

        /** The clauses. */
        Queue clauses = new LinkedList();

        /** The children. */
        List<QueryComponent> children = new ArrayList<MongoDBQuery.QueryComponent>();

        /** The parent. */
        QueryComponent parent;

        /** The actual query. */
        BasicDBObject actualQuery;
    }

    /**
     * Populate query components.
     * 
     * @param m
     *            the m
     * @param sq
     *            the sq
     */
    private void populateQueryComponents(EntityMetadata m, QueryComponent sq)
    {
        boolean hasChildren = false;
        if (sq.children != null && sq.children.size() > 0)
        {
            hasChildren = true;
            for (QueryComponent subQ : sq.children)
            {
                populateQueryComponents(m, subQ);
            }
        }
        if (sq.clauses.size() > 0 || hasChildren)
        {
            if (sq.clauses.size() > 0)
                sq.actualQuery = createSubMongoQuery(m, sq.clauses);
            if (hasChildren)
            {
                List<BasicDBObject> childQs = new ArrayList<BasicDBObject>();
                if (sq.clauses.size() > 0)
                    childQs.add(sq.actualQuery);
                for (QueryComponent subQ : sq.children)
                {
                    childQs.add(subQ.actualQuery);
                }
                if (sq.isAnd)
                {
                    BasicDBObject dbo = new BasicDBObject("$and", childQs);
                    sq.actualQuery = dbo;
                }
                else
                {
                    BasicDBObject dbo = new BasicDBObject("$or", childQs);
                    sq.actualQuery = dbo;
                }
            }
        }
        return;
    }

    /**
     * Gets the query component.
     * 
     * @param filterClauseQueue
     *            the filter clause queue
     * @return the query component
     */
    private static QueryComponent getQueryComponent(Queue filterClauseQueue)
    {
        QueryComponent subQuery = new QueryComponent();
        QueryComponent currentSubQuery = subQuery;

        for (Object object : filterClauseQueue)
        {
            if (object instanceof FilterClause)
            {
                currentSubQuery.clauses.add(object);
            }
            else if (object instanceof String)
            {
                String interClauseConstruct = (String) object;
                if (interClauseConstruct.equals("("))
                {
                    QueryComponent temp = new QueryComponent();
                    currentSubQuery.children.add(temp);
                    temp.parent = currentSubQuery;
                    currentSubQuery = temp;
                }
                else if (interClauseConstruct.equals(")"))
                {
                    currentSubQuery = currentSubQuery.parent;
                }
                else if (interClauseConstruct.equalsIgnoreCase("AND"))
                {
                    currentSubQuery.isAnd = true;
                }
                else if (interClauseConstruct.equalsIgnoreCase("OR"))
                {
                    currentSubQuery.isAnd = false;
                }
            }
        }

        return subQuery;
    }

    /**
     * Creates the mongo query.
     * 
     * @param m
     *            the m
     * @param filterClauseQueue
     *            the filter clause queue
     * @return the basic db object
     */
    public BasicDBObject createMongoQuery(EntityMetadata m, Queue filterClauseQueue)
    {
        QueryComponent sq = getQueryComponent(filterClauseQueue);
        populateQueryComponents(m, sq);
        return sq.actualQuery == null ? new BasicDBObject() : sq.actualQuery;
    }

    /**
     * Creates MongoDB Query object from filterClauseQueue.
     * 
     * @param m
     *            the m
     * @param filterClauseQueue
     *            the filter clause queue
     * @return the basic db object
     */
    public BasicDBObject createSubMongoQuery(EntityMetadata m, Queue filterClauseQueue)
    {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject compositeColumns = new BasicDBObject();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(m.getEntityClazz());

        for (Object object : filterClauseQueue)
        {
            boolean isCompositeColumn = false;

            boolean isSubCondition = false;

            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                String property = filter.getProperty();
                String condition = filter.getCondition();
                Object value = filter.getValue().get(0);

                // value is string but field.getType is different, then get
                // value using

                Field f = null;

                // if alias is still present .. means it is an enclosing
                // document search.

                if (managedType.hasLobAttribute())
                {
                    EntityType entity = metaModel.entity(m.getEntityClazz());
                    String fieldName = m.getFieldName(property);

                    f = (Field) entity.getAttribute(fieldName).getJavaMember();

                    if (value.getClass().isAssignableFrom(String.class) && f != null
                            && !f.getType().equals(value.getClass()))
                    {
                        value = PropertyAccessorFactory.getPropertyAccessor(f).fromString(f.getType().getClass(),
                                value.toString());
                    }
                    value = MongoDBUtils.populateValue(value, value.getClass());

                    property = "metadata." + property;
                }
                else
                {
                    if (((AbstractAttribute) m.getIdAttribute()).getJPAColumnName().equalsIgnoreCase(property))
                    {
                        property = "_id";
                        f = (Field) m.getIdAttribute().getJavaMember();
                        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                                && value.getClass().isAssignableFrom(f.getType()))
                        {
                            EmbeddableType compoundKey = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
                            compositeColumns = MongoDBUtils.getCompoundKeyColumns(m, value, compoundKey);
                            isCompositeColumn = true;
                            continue;
                        }
                    }
                    else if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                            && StringUtils.contains(property, '.'))
                    {
                        // Means it is a case of composite column.
                        property = property.substring(property.indexOf(".") + 1);
                        isCompositeColumn = true;
                    } /*
                       * if a composite key. "." assuming "." is part of
                       * property in case of embeddable only
                       */
                    else if (StringUtils.contains(property, '.'))
                    {
                        EntityType entity = metaModel.entity(m.getEntityClazz());
                        StringTokenizer tokenizer = new StringTokenizer(property, ".");
                        String embeddedAttributeAsStr = tokenizer.nextToken();
                        String embeddableAttributeAsStr = tokenizer.nextToken();
                        Attribute embeddedAttribute = entity.getAttribute(embeddedAttributeAsStr);
                        EmbeddableType embeddableEntity = metaModel.embeddable(((AbstractAttribute) embeddedAttribute)
                                .getBindableJavaType());
                        f = (Field) embeddableEntity.getAttribute(embeddableAttributeAsStr).getJavaMember();
                        property = ((AbstractAttribute) embeddedAttribute).getJPAColumnName()
                                + "."
                                + ((AbstractAttribute) embeddableEntity.getAttribute(embeddableAttributeAsStr))
                                        .getJPAColumnName();
                    }
                    else
                    {
                        EntityType entity = metaModel.entity(m.getEntityClazz());
                        String discriminatorColumn = ((AbstractManagedType) entity).getDiscriminatorColumn();

                        if (!property.equals(discriminatorColumn))
                        {
                            String fieldName = m.getFieldName(property);
                            f = (Field) entity.getAttribute(fieldName).getJavaMember();
                        }
                    }
                    if (value.getClass().isAssignableFrom(String.class) && f != null
                            && !f.getType().equals(value.getClass()))
                    {
                        value = PropertyAccessorFactory.getPropertyAccessor(f).fromString(f.getType().getClass(),
                                value.toString());
                    }
                    value = MongoDBUtils.populateValue(value, value.getClass());

                }

                // Property, if doesn't exist in entity, may be there in a
                // document embedded within it, so we have to check that
                // TODO: Query should actually be in a format
                // documentName.embeddedDocumentName.column, remove below if
                // block once this is decided

                // Query could be geospatial in nature
                if (f != null && f.getType().equals(Point.class))
                {
                    GeospatialQuery geospatialQueryimpl = GeospatialQueryFactory.getGeospatialQueryImplementor(
                            condition, value);
                    query = (BasicDBObject) geospatialQueryimpl.createGeospatialQuery(property, value, query);

                }
                else
                {

                    if (isCompositeColumn)
                    {
                        EmbeddableType embeddableType = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
                        AbstractAttribute attribute = (AbstractAttribute) embeddableType.getAttribute(property);

                        property = new StringBuffer("_id.").append(attribute.getJPAColumnName()).toString();
                    }
                    if (condition.equals("="))
                    {
                        query.append(property, value);

                    }
                    else if (condition.equalsIgnoreCase("like"))
                    {

                        if (query.containsField(property))
                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$regex",
                                    createLikeRegex((String) value)));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$regex", createLikeRegex((String) value)));
                        }

                    }
                    else if (condition.equalsIgnoreCase(">"))
                    {

                        if (query.containsField(property))
                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$gt", value));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$gt", value));
                        }
                    }
                    else if (condition.equalsIgnoreCase(">="))
                    {

                        if (query.containsField(property))

                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$gte", value));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$gte", value));
                        }

                    }
                    else if (condition.equalsIgnoreCase("<"))
                    {

                        if (query.containsField(property))
                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$lt", value));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$lt", value));
                        }

                    }
                    else if (condition.equalsIgnoreCase("<="))
                    {

                        if (query.containsField(property))
                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$lte", value));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$lte", value));
                        }

                    }
                    else if (condition.equalsIgnoreCase("in"))
                    {

                        if (query.containsField(property))
                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$in", filter.getValue()));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$in", filter.getValue()));
                        }

                    }
                    else if (condition.equalsIgnoreCase("not in"))
                    {

                        if (query.containsField(property))
                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$nin", filter.getValue()));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$nin", filter.getValue()));
                        }

                    }
                    else if (condition.equalsIgnoreCase("<>"))
                    {

                        if (query.containsField(property))
                        {
                            query.get(property);
                            query.put(property, ((BasicDBObject) query.get(property)).append("$ne", value));
                        }
                        else
                        {
                            query.append(property, new BasicDBObject("$ne", value));
                        }

                    }
                }

                // TODO: Add support for other operators like >, <, >=, <=,
                // order by asc/ desc, limit, skip, count etc
            }
        }
        if (!compositeColumns.isEmpty())
        {
            query.append("_id", compositeColumns);
        }

        return query;
    }

    /**
     * Gets the keys.
     * 
     * @param m
     *            the m
     * @param columns
     *            the columns
     * @return the keys
     */
    private BasicDBObject getKeys(EntityMetadata m, String[] columns)
    {
        BasicDBObject keys = new BasicDBObject();
        if (columns != null && columns.length > 0)
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entity = metaModel.entity(m.getEntityClazz());
            for (int i = 1; i < columns.length; i++)
            {
                if (columns[i] != null)
                {
                    Attribute col = entity.getAttribute(columns[i]);
                    if (col == null)
                    {
                        throw new QueryHandlerException("column type is null for: " + columns);
                    }
                    keys.put(((AbstractAttribute) col).getJPAColumnName(), 1);
                }
            }
        }
        return keys;
    }

    /**
     * Prepare order by clause.
     * 
     * @param metadata
     *            the metadata
     * @return order by clause.
     */
    private BasicDBObject getOrderByClause(final EntityMetadata metadata)
    {

        BasicDBObject orderByClause = null;
        Metamodel metaModel = kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());

        List<SortOrdering> orders = kunderaQuery.getOrdering();
        if (orders != null)
        {
            orderByClause = new BasicDBObject();
            if (!managedType.hasLobAttribute())
            {
                for (SortOrdering order : orders)
                {
                    orderByClause.append(getColumnName(metadata, entityType, order.getColumnName()), order.getOrder()
                            .equals(SortOrder.ASC) ? 1 : -1);
                }
            }
            else
            {
                for (SortOrdering order : orders)
                {
                    orderByClause.append("metadata." + getColumnName(metadata, entityType, order.getColumnName()),
                            order.getOrder().equals(SortOrder.ASC) ? 1 : -1);
                }
            }
        }

        return orderByClause;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        int ret = handleSpecialFunctions();
        if (ret == -1)
        {
            return onUpdateDeleteEvent();
        }
        return ret;
    }

    /** The Constant SINGLE_STRING_KEYWORDS. */
    public static final String[] FUNCTION_KEYWORDS = { "INCREMENT\\(\\d+\\)", "DECREMENT\\(\\d+\\)" };

    /**
     * Handle special functions.
     * 
     * @return the int
     */
    private int handleSpecialFunctions()
    {

        boolean needsSpecialAttention = false;
        outer: for (UpdateClause c : kunderaQuery.getUpdateClauseQueue())
        {
            for (int i = 0; i < FUNCTION_KEYWORDS.length; i++)
            {
                if (c.getValue() instanceof String)
                {
                    String func = c.getValue().toString();
                    func = func.replaceAll(" ", "");
                    if (func.toUpperCase().matches(FUNCTION_KEYWORDS[i]))
                    {
                        needsSpecialAttention = true;
                        c.setValue(func);
                        break outer;
                    }
                }
            }
        }

        if (!needsSpecialAttention)
            return -1;

        EntityMetadata m = getEntityMetadata();
        Metamodel metaModel = kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());
        Queue filterClauseQueue = kunderaQuery.getFilterClauseQueue();
        BasicDBObject query = createMongoQuery(m, filterClauseQueue);

        BasicDBObject update = new BasicDBObject();
        for (UpdateClause c : kunderaQuery.getUpdateClauseQueue())
        {
            String columName = getColumnName(m, metaModel.entity(m.getEntityClazz()), c.getProperty());
            boolean isSpecialFunction = false;
            for (int i = 0; i < FUNCTION_KEYWORDS.length; i++)
            {

                if (c.getValue() instanceof String
                        && c.getValue().toString().toUpperCase().matches(FUNCTION_KEYWORDS[i]))
                {
                    isSpecialFunction = true;

                    if (c.getValue().toString().toUpperCase().startsWith("INCREMENT("))
                    {
                        String val = c.getValue().toString().toUpperCase();
                        val = val.substring(10, val.indexOf(")"));
                        update.put("$inc", new BasicDBObject(columName, Integer.valueOf(val)));
                    }
                    else if (c.getValue().toString().toUpperCase().startsWith("DECREMENT("))
                    {
                        String val = c.getValue().toString().toUpperCase();
                        val = val.substring(10, val.indexOf(")"));
                        update.put("$inc", new BasicDBObject(columName, -Integer.valueOf(val)));
                    }
                }
            }
            if (!isSpecialFunction)
            {
                update.put(columName, c.getValue());
            }
        }

        Client client = persistenceDelegeator.getClient(m);
        return ((MongoDBClient) client).handleUpdateFunctions(query, update, m.getTableName());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#close()
     */
    @Override
    public void close()
    {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#iterate()
     */
    @Override
    public Iterator iterate()
    {
        EntityMetadata m = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);

        int fetchSize;
        if (getFetchSize() != null)
        {
            fetchSize = getFetchSize();
        }
        else if (this.maxResult != 0)
        {
            fetchSize = this.maxResult;
        }
        else
        {
            fetchSize = Integer.MAX_VALUE;
        }

        return new ResultIterator((MongoDBClient) client, m, createMongoQuery(m, getKunderaQuery()
                .getFilterClauseQueue()), getOrderByClause(m), getKeys(m, getKunderaQuery().getResult()),
                persistenceDelegeator, fetchSize);
    }

    /**
     * Gets the column name.
     * 
     * @param metadata
     *            the metadata
     * @param entityType
     *            the entity type
     * @param property
     *            the property
     * @return the column name
     */
    private String getColumnName(EntityMetadata metadata, EntityType entityType, String property)
    {
        String columnName = null;

        if (property.indexOf(".") > 0)
        {
            property = property.substring((kunderaQuery.getEntityAlias() + ".").length());
        }
        try
        {
            columnName = ((AbstractAttribute) entityType.getAttribute(property)).getJPAColumnName();
        }
        catch (IllegalArgumentException iaex)
        {
            log.warn("No column found by this name : " + property + " checking for embeddedfield");
        }
        // where condition may be for search within embedded object
        if (columnName == null && property.indexOf(".") > 0)
        {
            String enclosingEmbeddedField = MetadataUtils.getEnclosingEmbeddedFieldName(metadata, property, true,
                    kunderaMetadata);
            if (enclosingEmbeddedField != null)
            {
                columnName = property;
            }
        }

        if (columnName == null)
        {
            log.error("No column found by this name : " + property);
            throw new JPQLParseException("No column found by this name : " + property + ". Check your query.");
        }
        return columnName;
    }

    /**
     * Create regular expression equivalent to any like operator string match
     * function.
     * 
     * @param expr
     *            the expr
     * @return the string
     */
    public static String createLikeRegex(String expr)
    {
        String regex = createRegex(expr);
        regex = regex.replace("_", ".").replace("%", ".*?");

        return regex;
    }

    /**
     * Generates the regular expression for matching string for like operator.
     * 
     * @param value
     *            the value
     * @return the string
     */
    public static String createRegex(String value)
    {
        if (value == null)
        {
            throw new IllegalArgumentException("String cannot be null");
        }

        int len = value.length();
        if (len == 0)
        {
            return "";
        }

        StringBuilder sb = new StringBuilder(len * 2);
        sb.append("(?i)^");
        for (int i = 0; i < len; i++)
        {
            char c = value.charAt(i);
            if ("[](){}.*+?$^|#\\".indexOf(c) != -1)
            {
                sb.append("\\");
            }
            sb.append(c);
        }
        sb.append("$");
        return sb.toString();
    }

}