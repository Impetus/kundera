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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.lang.StringUtils;
import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.CountFunction;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
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
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.attributes.DefaultSingularAttribute;
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
import com.mongodb.DBObject;
import com.mongodb.BasicDBList;
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
                if (kunderaQuery.isAggregated())
                {
                    return ((MongoDBClient) client).aggregate(m,
                            createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), createAggregationLookup(m),
                            createAggregation(m), getAggregationOrderByClause(m), isSingleResult ? 1 : maxResult);
                }
                else
                {
                    BasicDBObject orderByClause = getOrderByClause(m);
                    return ((MongoDBClient) client).loadData(m,
                            createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), null, orderByClause,
                            isSingleResult ? 1 : maxResult, firstResult, isCountQuery(),
                            getKeys(m, getKunderaQuery().getResult()), getKunderaQuery().getResult());
                }
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
     * @see com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.
     * impetus .kundera.metadata.model.EntityMetadata,
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
                if (kunderaQuery.isAggregated())
                {
                    return ((MongoDBClient) client).aggregate(m,
                            createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), createAggregationLookup(m),
                            createAggregation(m), getAggregationOrderByClause(m), isSingleResult ? 1 : maxResult);
                }
                else
                {
                    BasicDBObject orderByClause = getOrderByClause(m);
                    ls = ((MongoDBClient) client).loadData(m,
                            createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), m.getRelationNames(),
                            orderByClause, isSingleResult ? 1 : maxResult, firstResult, isCountQuery(),
                            getKeys(m, getKunderaQuery().getResult()), getKunderaQuery().getResult());
                }
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
            {
                sq.actualQuery = createSubMongoQuery(m, sq.clauses);
            }
            if (hasChildren)
            {
                List<BasicDBObject> childQs = new ArrayList<BasicDBObject>();
                if (sq.clauses.size() > 0)
                    childQs.add(sq.actualQuery);
                for (QueryComponent subQ : sq.children)
                {
                    childQs.add(subQ.actualQuery);
                }
                if (childQs.size() == 1)
                {
                    sq.actualQuery = childQs.get(0);
                }
                else if (sq.isAnd)
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

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(m.getPersistenceUnit());

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
                boolean ignoreCase = filter.isIgnoreCase();

                Object value;
                if (filter.getValue().size() == 1)
                {
                    value = filter.getValue().get(0);
                }
                else
                {
                    value = filter.getValue();
                }

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

                    if (value != null)
                    {
                        if (value.getClass().isAssignableFrom(String.class) && f != null
                                && !f.getType().equals(value.getClass()))
                        {
                            value = PropertyAccessorFactory.getPropertyAccessor(f).fromString(f.getType().getClass(),
                                    value.toString());
                        }
                        value = MongoDBUtils.populateValue(value, value.getClass());
                    }

                    property = "metadata." + property;
                }
                else
                {
                    if (((AbstractAttribute) m.getIdAttribute()).getJPAColumnName().equalsIgnoreCase(property))
                    {
                        if (value != null)
                        {
                            property = "_id";
                            f = (Field) m.getIdAttribute().getJavaMember();
                            if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                                    && value.getClass().isAssignableFrom(f.getType()))
                            {
                                EmbeddableType compoundKey = metaModel
                                        .embeddable(m.getIdAttribute().getBindableJavaType());
                                compositeColumns = MongoDBUtils.getCompoundKeyColumns(m, value, compoundKey, metaModel);
                                isCompositeColumn = true;
                                continue;
                            }
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

                        if (embeddedAttribute.isAssociation())
                        {
                            Relation relation = m.getRelation(embeddedAttributeAsStr);
                            f = relation.getProperty();

                            AbstractAttribute targetAttribute = (AbstractAttribute) metaModel
                                    .entity(relation.getTargetEntity()).getAttribute(embeddableAttributeAsStr);
                            String targetColumnName = targetAttribute.getJPAColumnName();

                            if (targetAttribute instanceof DefaultSingularAttribute
                                    && ((DefaultSingularAttribute) targetAttribute).isId())
                            {
                                property = relation.getJoinColumnName(kunderaMetadata);
                            }
                            else
                            {
                                property = embeddedAttributeAsStr + "." + targetColumnName;
                            }
                        }
                        else
                        {
                            EmbeddableType embeddableEntity = metaModel
                                    .embeddable(((AbstractAttribute) embeddedAttribute).getBindableJavaType());
                            f = (Field) embeddableEntity.getAttribute(embeddableAttributeAsStr).getJavaMember();
                            property = ((AbstractAttribute) embeddedAttribute).getJPAColumnName() + "."
                                    + ((AbstractAttribute) embeddableEntity.getAttribute(embeddableAttributeAsStr))
                                            .getJPAColumnName();
                        }
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

                    if (value != null)
                    {
                        if (value.getClass().isAssignableFrom(String.class) && f != null
                                && !f.getType().equals(value.getClass()))
                        {
                            value = PropertyAccessorFactory.getPropertyAccessor(f).fromString(f.getType(),
                                    value.toString());
                        }
                        value = MongoDBUtils.populateValue(value, value.getClass());
                    }

                }

                // Property, if doesn't exist in entity, may be there in a
                // document embedded within it, so we have to check that
                // TODO: Query should actually be in a format
                // documentName.embeddedDocumentName.column, remove below if
                // block once this is decided

                // Query could be geospatial in nature
                if (f != null && f.getType().equals(Point.class))
                {
                    GeospatialQuery geospatialQueryimpl = GeospatialQueryFactory
                            .getGeospatialQueryImplementor(condition, value);
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

                    if (ignoreCase)
                    {
                        // let 'like' and 'not like' take care of this on its
                        // own
                        if (!condition.equalsIgnoreCase("like") && !condition.equalsIgnoreCase("not like"))
                        {

                            if (value instanceof String)
                            {
                                value = Pattern.compile(createLikeRegex((String) value, ignoreCase));

                            }
                            else if (value instanceof Collection)
                            {
                                Collection<?> original = (Collection<?>) value;
                                List<Pattern> values = new ArrayList<Pattern>(original.size());

                                for (Object item : original)
                                {
                                    values.add(Pattern.compile(createLikeRegex((String) item, ignoreCase)));
                                }

                                value = values;

                            }
                        }
                    }

                    if (condition.equals("="))
                    {

                        if (query.containsField(property))
                        {
                            appendToQuery(query, property, "$eq", value);
                        }
                        else
                        {
                            query.append(property, value);
                        }

                    }
                    else if (condition.equalsIgnoreCase("is null"))
                    {
                        if (query.containsField(property))
                        {
                            appendToQuery(query, property, "$eq", null);
                        }
                        else
                        {
                            query.append(property, null);
                        }
                    }
                    else if (condition.toLowerCase().contains("like"))
                    {

                        Pattern regEx = Pattern.compile(createLikeRegex((String) value, ignoreCase));
                        boolean negative = condition.toLowerCase().contains("not");

                        appendToQuery(query, property, negative ? "$not" : "$regex", regEx);

                    }
                    else if (condition.equalsIgnoreCase(">"))
                    {

                        appendToQuery(query, property, "$gt", value);

                    }
                    else if (condition.equalsIgnoreCase(">="))
                    {

                        appendToQuery(query, property, "$gte", value);

                    }
                    else if (condition.equalsIgnoreCase("<"))
                    {

                        appendToQuery(query, property, "$lt", value);

                    }
                    else if (condition.equalsIgnoreCase("<="))
                    {

                        appendToQuery(query, property, "$lte", value);

                    }
                    else if (condition.equalsIgnoreCase("in"))
                    {

                        if (value != null)
                        {
                            if (!value.getClass().isArray() && !(value instanceof Collection))
                            {
                                value = Collections.singletonList(value);
                            }
                        }

                        appendToQuery(query, property, "$in", value);

                    }
                    else if (condition.equalsIgnoreCase("not in"))
                    {

                        if (value != null)
                        {
                            if (!value.getClass().isArray() && !(value instanceof Collection))
                            {
                                value = Collections.singletonList(value);
                            }
                        }

                        appendToQuery(query, property, "$nin", value);

                    }
                    else if (condition.equalsIgnoreCase("<>"))
                    {

                        String operator = value instanceof Pattern ? "$not" : "$ne";

                        appendToQuery(query, property, operator, value);

                    }
                    else if (condition.equalsIgnoreCase("is not null"))
                    {

                        appendToQuery(query, property, "$not", null);

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

    private void appendToQuery(BasicDBObject query, String property, String operator, Object value)
    {
        if (query.containsField(property))
        {
            Object existing = query.get(property);

            if (!(existing instanceof BasicDBObject))
            {
                query.put(property, new BasicDBObject("$eq", existing));
            }

            query.put(property, ((BasicDBObject) query.get(property)).append(operator, value));
        }
        else
        {
            query.append(property, new BasicDBObject(operator, value));
        }
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
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                    .getMetamodel(m.getPersistenceUnit());
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

    private BasicDBList createAggregationLookup(EntityMetadata metadata)
    {
        BasicDBList lookup = new BasicDBList();

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(metadata.getPersistenceUnit());

        for (Relation relation : metadata.getRelations())
        {
            EntityType entityType = metaModel.entity(metadata.getEntityClazz());

            AbstractAttribute attribute = (AbstractAttribute) entityType.getAttribute(relation.getProperty().getName());
            boolean optional = false;
            if (attribute instanceof SingularAttribute)
            {
                optional = ((SingularAttribute) attribute).isOptional();
            }

            EntityMetadata associatedMetadata = metaModel.getEntityMetadata(relation.getTargetEntity());
            EntityType associatedEntityType = metaModel.entity(associatedMetadata.getEntityClazz());

            String joinColumn = relation.getJoinColumnName(kunderaMetadata);
            String localField = metadata.getFieldName(joinColumn);
            String foreignField = associatedMetadata.getFieldName(joinColumn);
            Attribute localAttribute = entityType.getAttribute(localField);

            if (foreignField != null)
            {
                Attribute foreignAttribute = associatedEntityType.getAttribute(foreignField);
                foreignField = getActualColumnName(foreignAttribute);
            }
            else
            {
                foreignField = "_id";
            }

            BasicDBObject item = new BasicDBObject();
            item.put("from", associatedMetadata.getTableName());
            item.put("localField", getActualColumnName(localAttribute));
            item.put("foreignField", foreignField);
            item.put("as", attribute.getName());
            lookup.add(new BasicDBObject("$lookup", item));

            if (Arrays.asList(Relation.ForeignKey.ONE_TO_ONE, Relation.ForeignKey.MANY_TO_ONE)
                    .contains(relation.getType()))
            {
                BasicDBObject unwind = new BasicDBObject();
                unwind.append("path", "$" + attribute.getName());
                unwind.append("preserveNullAndEmptyArrays", optional);
                lookup.add(new BasicDBObject("$unwind", unwind));
            }
        }

        return lookup;
    }

    private String getActualColumnName(Attribute attribute)
    {
        if (attribute instanceof DefaultSingularAttribute)
        {
            DefaultSingularAttribute dsAttribute = (DefaultSingularAttribute) attribute;

            if (dsAttribute.isId())
            {
                return "_id";
            }
            else
            {
                return dsAttribute.getJPAColumnName();
            }
        }
        else if (attribute instanceof AbstractAttribute)
        {
            return ((AbstractAttribute) attribute).getJPAColumnName();
        }
        else
        {
            return attribute.getName();
        }
    }

    /**
     * Get the aggregation object.
     *
     * @param metadata
     * @return
     */
    private BasicDBObject createAggregation(EntityMetadata metadata)
    {
        if (kunderaQuery.getSelectStatement() != null)
        {
            Metamodel metaModel = kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
            EntityType entityType = metaModel.entity(metadata.getEntityClazz());
            AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());
            boolean hasLob = managedType.hasLobAttribute();

            BasicDBObject aggregation = new BasicDBObject();

            SelectClause selectClause = (SelectClause) kunderaQuery.getSelectStatement().getSelectClause();
            Expression expression = selectClause.getSelectExpression();

            buildAggregation(aggregation, expression, metadata, entityType, hasLob);

            if (aggregation.size() == 0)
            {
                return null;
            }

            if (!aggregation.containsField("_id"))
            {
                aggregation.put("_id", null);
            }

            return aggregation;
        }

        return null;
    }

    /**
     * Build the aggregation parameters.
     *
     * @param group
     * @param expression
     * @param metadata
     * @param entityType
     */
    private void buildAggregation(DBObject group, Expression expression, EntityMetadata metadata, EntityType entityType,
            boolean hasLob)
    {
        if (expression instanceof AggregateFunction)
        {
            AggregateFunction aggregateFunction = (AggregateFunction) expression;
            String identifier = aggregateFunction.getIdentifier().toLowerCase();

            Expression child = aggregateFunction.getExpression();

            if (child instanceof StateFieldPathExpression)
            {
                StateFieldPathExpression sfpExp = (StateFieldPathExpression) child;
                String columnName = getColumnName(metadata, entityType, sfpExp.toActualText());
                String actualColumnName = columnName;
                if (hasLob)
                {
                    actualColumnName = "metadata." + columnName;
                }
                else if (metadata.getIdAttribute().equals(entityType.getAttribute(metadata.getFieldName(columnName))))
                {
                    actualColumnName = "_id";
                }

                BasicDBObject item = new BasicDBObject("$" + identifier, "$" + actualColumnName);
                group.put(identifier + "_" + columnName, item);
            }
            else if (expression instanceof CountFunction)
            {
                group.put("count", new BasicDBObject("$sum", 1));
            }
        }
        else if (expression instanceof CollectionExpression)
        {
            for (Expression child : expression.children())
            {
                buildAggregation(group, child, metadata, entityType, hasLob);
            }
        }
        else if (expression instanceof StateFieldPathExpression)
        {
            StateFieldPathExpression sfpExp = (StateFieldPathExpression) expression;

            BasicDBObject idObject;
            Object existing = group.get("_id");
            if (existing != null)
            {
                idObject = (BasicDBObject) existing;
            }
            else
            {
                idObject = new BasicDBObject();
                group.put("_id", idObject);
            }

            String columnName = getColumnName(metadata, entityType, sfpExp.toActualText());
            String actualColumnName = columnName;
            if (hasLob)
            {
                actualColumnName = "metadata." + columnName;
            }

            idObject.put(columnName, "$" + actualColumnName);
        }
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
                    orderByClause.append(getColumnName(metadata, entityType, order.getColumnName()),
                            order.getOrder().equals(SortOrder.ASC) ? 1 : -1);
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

    private BasicDBObject getAggregationOrderByClause(final EntityMetadata metadata)
    {
        BasicDBObject orderByClause = null;
        Metamodel metaModel = kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(metadata.getEntityClazz());

        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(metadata.getEntityClazz());

        List<SortOrdering> orders = kunderaQuery.getOrdering();
        if (orders != null)
        {
            orderByClause = new BasicDBObject();

            for (SortOrdering order : orders)
            {
                if (order.getColumnName().contains("("))
                {
                    String function = order.getColumnName().replaceFirst("\\s*(.*?)\\s*\\(.*", "$1");
                    String property = order.getColumnName().replaceFirst(".*?\\(\\s*(.*)\\s*\\).*", "$1");
                    String columnName = getColumnName(metadata, entityType, property);

                    orderByClause.append(function.toLowerCase() + "_" + columnName,
                            order.getOrder().equals(SortOrder.ASC) ? 1 : -1);
                }
                else
                {
                    if (!managedType.hasLobAttribute())
                    {
                        orderByClause.append(getColumnName(metadata, entityType, order.getColumnName()),
                                order.getOrder().equals(SortOrder.ASC) ? 1 : -1);
                    }
                    else
                    {
                        orderByClause.append("metadata." + getColumnName(metadata, entityType, order.getColumnName()),
                                order.getOrder().equals(SortOrder.ASC) ? 1 : -1);
                    }
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
        return new ResultIterator((MongoDBClient) client, m,
                createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), getOrderByClause(m),
                getKeys(m, getKunderaQuery().getResult()), persistenceDelegeator,
                getFetchSize() != null ? getFetchSize() : this.maxResult);
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
     * @param ignoreCase
     *            whether to ignore the case
     * @return the string
     */
    public static String createLikeRegex(String expr, boolean ignoreCase)
    {
        String regex = createRegex(expr, ignoreCase);
        regex = regex.replace("_", ".").replace("%", ".*?");

        return regex;
    }

    /**
     * Generates the regular expression for matching string for like operator.
     * 
     * @param value
     *            the value
     * @param ignoreCase
     *            whether to ignore the case
     * @return the string
     */
    public static String createRegex(String value, boolean ignoreCase)
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

        if (ignoreCase)
        {
            sb.append("(?i)");
        }

        sb.append("^");

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