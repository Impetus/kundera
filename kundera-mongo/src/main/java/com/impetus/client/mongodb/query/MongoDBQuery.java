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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Queue;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.mongodb.MongoEntityReader;
import com.impetus.client.mongodb.query.gis.GeospatialQueryFactory;
import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.gis.query.GeospatialQuery;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.SortOrder;
import com.impetus.kundera.query.KunderaQuery.SortOrdering;
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
    private static Log log = LogFactory.getLog(MongoDBQuery.class);

    /**
     * Instantiates a new mongo db query.
     * 
     * @param jpaQuery
     *            the jpa query
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     */
    public MongoDBQuery(String jpaQuery, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(jpaQuery, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
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
        try
        {
            BasicDBObject orderByClause = getOrderByClause();
            return ((MongoDBClient) client).loadData(m, createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()),
                    null, orderByClause, getKeys(m, getKunderaQuery().getResult()), getKunderaQuery().getResult());
        }
        catch (Exception e)
        {
            log.error("Error during executing query, Caused by:" + e.getMessage());
            throw new QueryHandlerException(e);
        }
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        // TODO : required to modify client return relation.
        // if it is a parent..then find data related to it only
        // else u need to load for associated fields too.
        List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();
        try
        {
            BasicDBObject orderByClause = getOrderByClause();
            ls = ((MongoDBClient) client).loadData(m, createMongoQuery(m, getKunderaQuery().getFilterClauseQueue()), m
                    .getRelationNames(), orderByClause, getKeys(m, getKunderaQuery().getResult()), getKunderaQuery()
                    .getResult());
        }
        catch (Exception e)
        {
            log.error("Error during executing query, Caused by:" + e.getMessage());
            throw new QueryHandlerException(e);
        }

        return setRelationEntities(ls, client, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return new MongoEntityReader();
    }

    /**
     * Creates MongoDB Query object from filterClauseQueue.
     * 
     * @param m
     *            the m
     * @param filterClauseQueue
     *            the filter clause queue
     * @param columns
     * @return the basic db object
     */
    private BasicDBObject createMongoQuery(EntityMetadata m, Queue filterClauseQueue)
    {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject compositeColumns = new BasicDBObject();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        for (Object object : filterClauseQueue)
        {
            boolean isCompositeColumn = false;

            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                // String property = getColumnName(filter.getProperty());
                String property = filter.getProperty();
                String condition = filter.getCondition();
                // String value = filter.getValue().toString();
                Object value = filter.getValue();

                // value is string but field.getType is different, then get
                // value using

                Field f = null;

                // if alias is still present .. means it is an enclosing
                // document search.
                //

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
                    // compositeColumns.add(new
                    // BasicDBObject(compositeColumn,value));
                }
                else
                {
                    // MetamodelImpl metaModel = (MetamodelImpl)
                    // KunderaMetadata.INSTANCE.getApplicationMetadata()
                    // .getMetamodel(m.getPersistenceUnit());

                    EntityType entity = metaModel.entity(m.getEntityClazz());
                    String fieldName = m.getFieldName(property);
                    f = (Field) entity.getAttribute(fieldName).getJavaMember();
                }

                if (value.getClass().isAssignableFrom(String.class) && f != null
                        && !f.getType().equals(value.getClass()))
                {
                    value = PropertyAccessorFactory.getPropertyAccessor(f).fromString(f.getType().getClass(),
                            value.toString());
                }
                value = populateValue(value, value.getClass());

                // Property, if doesn't exist in entity, may be there in a
                // document embedded within it, so we have to check that
                // TODO: Query should actually be in a format
                // documentName.embeddedDocumentName.column, remove below if
                // block once this is decided

                // String enclosingDocumentName =
                // MetadataUtils.getEnclosingEmbeddedFieldName(m, property,
                // true);
                // if (enclosingDocumentName != null)
                // {
                // property = enclosingDocumentName + "." + property;
                // }

                // Query could be geospatial in nature
                if (f != null && f.getType().equals(Point.class))
                {
                    GeospatialQuery geospatialQueryimpl = GeospatialQueryFactory.getGeospatialQueryImplementor(condition, value);
                    query = (BasicDBObject)geospatialQueryimpl.createGeospatialQuery(property, value, query);      

                }
                else
                {
                    if (condition.equals("="))
                    {
                        if (isCompositeColumn)
                        {
                            compositeColumns.put(property, value);
                        }
                        else
                        {
                            query.append(property, value);
                        }

                    }
                    else if (condition.equalsIgnoreCase("like"))
                    {
                        // query.append(property, Pattern.compile(value));
                        if (isCompositeColumn)
                        {
                            compositeColumns.put(property, value);
                        }
                        else
                        {
                            query.append(property, value);
                        }
                    }
                    else if (condition.equalsIgnoreCase(">"))
                    {
                        if (isCompositeColumn)
                        {
                            compositeColumns.put(property, new BasicDBObject("$gt", value));

                        }
                        else
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

                    }
                    else if (condition.equalsIgnoreCase(">="))
                    {
                        if (isCompositeColumn)
                        {
                            compositeColumns.put(property, new BasicDBObject("$gte", value));

                        }
                        else
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
                    }
                    else if (condition.equalsIgnoreCase("<"))
                    {
                        if (isCompositeColumn)
                        {
                            compositeColumns.put(property, new BasicDBObject("$lt", value));

                        }
                        else
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
                    }
                    else if (condition.equalsIgnoreCase("<="))
                    {
                        if (isCompositeColumn)
                        {
                            compositeColumns.put(property, new BasicDBObject("$lte", value));

                        }
                        else
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

    private BasicDBObject getKeys(EntityMetadata m, String[] columns)
    {
        BasicDBObject keys = new BasicDBObject();
        if (columns != null && columns.length > 0)
        {
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
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
     * @return order by clause.
     */
    private BasicDBObject getOrderByClause()
    {
        BasicDBObject orderByClause = null;

        List<SortOrdering> orders = kunderaQuery.getOrdering();
        if (orders != null)
        {
            orderByClause = new BasicDBObject();
            for (SortOrdering order : orders)
            {
                orderByClause.append(order.getColumnName(), order.getColumnName().equals(SortOrder.ASC) ? 1 : -1);
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
        if (kunderaQuery.isDeleteUpdate())
        {
            List result = getResultList();
            return result != null ? result.size() : 0;
        }

        return 0;
    }

    /**
     * @param valObj
     * @return
     */
    public Object populateValue(Object valObj, Class clazz)
    {
        if (isUTF8Value(clazz))
        {
            return valObj.toString();
        }
        return valObj;
    }

    private boolean isUTF8Value(Class<?> clazz)
    {
        return (clazz.isAssignableFrom(BigDecimal.class))
                || (clazz.isAssignableFrom(BigInteger.class) || (clazz.isAssignableFrom(String.class))
                        || (clazz.isAssignableFrom(char.class)) || (clazz.isAssignableFrom(Character.class))
                        || (clazz.isAssignableFrom(Calendar.class)) || (clazz.isAssignableFrom(GregorianCalendar.class)));
    }

}
