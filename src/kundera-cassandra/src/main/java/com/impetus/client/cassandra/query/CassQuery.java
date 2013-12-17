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
package com.impetus.client.cassandra.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javassist.Modifier;

import javax.persistence.Transient;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.commons.lang.StringUtils;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.SortOrder;
import com.impetus.kundera.query.KunderaQuery.SortOrdering;
import com.impetus.kundera.query.KunderaQuery.UpdateClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * @author vivek.mishra
 * 
 *         Query implementation for Cassandra.
 */
@SuppressWarnings("unchecked")
public class CassQuery extends QueryImpl
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(CassQuery.class);

    /** The reader. */
    private EntityReader reader;

    private Map<String, Object> externalProperties;

    private boolean isSingleResult = false;

    /**
     * Instantiates a new cass query.
     * 
     * @param query
     *            the query
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     */
    public CassQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
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
        if (log.isDebugEnabled())
        {
            log.debug("Populating entities for Cassandra query {}.", getJPAQuery());
        }
        List<Object> result = new ArrayList<Object>();
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        externalProperties = ((CassandraClientBase) client).getExternalProperties();

        // if id attribute is embeddable, it is meant for CQL translation.
        // make it independent of embedded stuff and allow even to add non
        // composite into where clause and let cassandra complain for it.

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        String query = appMetadata.getQuery(getJPAQuery());
        boolean isNative = kunderaQuery.isNative();

        if (!isNative && ((CassandraClientBase) client).isCql3Enabled(m)
                && MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
        // if (!isNative && ((CassandraClientBase) client).isCql3Enabled(m))

        {
            result = ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), null,isNative,
                    onQueryOverCQL3(m, client, metaModel, null));
        }
        else
        {
            if (isNative)
            {
                result = ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), null,isNative, query != null ? query
                        : getJPAQuery());
            }
            else
            {
                if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
                {
                    // Index in Inverted Index table if applicable
                    boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m,
                            MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()));
                    Map<Boolean, List<IndexClause>> ixClause = prepareIndexClause(m, useInvertedIndex);
                    if (useInvertedIndex && !getKunderaQuery().getFilterClauseQueue().isEmpty())
                    {
                        result = (List) ((CassandraEntityReader) getReader()).readFromIndexTable(m, client, ixClause);
                    }
                    else
                    {
                        boolean isRowKeyQuery = ixClause.keySet().iterator().next();
                        if (!isRowKeyQuery)
                        {
                            result = ((CassandraClientBase) client).find(ixClause.get(isRowKeyQuery), m, false, null,
                                    isSingleResult ? 1 : this.maxResult,
                                    getColumnList(m, getKunderaQuery().getResult(), null));
                        }
                        else
                        {
                            result = ((CassandraEntityReader) getReader()).handleFindByRange(m, client, result,
                                    ixClause, isRowKeyQuery, getColumnList(m, getKunderaQuery().getResult(), null),
                                    isSingleResult ? 1 : this.maxResult);
                        }
                    }

                }
                else
                {
                    result = populateUsingLucene(m, client, result, null);
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<EnhanceEntity> ls = null;
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        String query = appMetadata.getQuery(getJPAQuery());
        boolean isNative = kunderaQuery.isNative();

        if (isNative)
        {
            ls = (List<EnhanceEntity>) ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), null,isNative,
                    query != null ? query : getJPAQuery());
        }
        else if (!isNative && ((CassandraClientBase) client).isCql3Enabled(m))
        {
            ls = ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), m.getRelationNames(),isNative,
                    onQueryOverCQL3(m, client, metaModel, m.getRelationNames()));
        }
        else
        {
            // Index in Inverted Index table if applicable
            boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m,
                    MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()));
            Map<Boolean, List<IndexClause>> ixClause = MetadataUtils.useSecondryIndex(((ClientBase) client)
                    .getClientMetadata()) ? prepareIndexClause(m, useInvertedIndex) : null;

            if (useInvertedIndex && !getKunderaQuery().getFilterClauseQueue().isEmpty())
            {
                ls = ((CassandraEntityReader) getReader()).readFromIndexTable(m, client, ixClause);
            }
            else
            {
                ((CassandraEntityReader) getReader()).setConditions(ixClause);
                ls = reader.populateRelation(m, client, isSingleResult ? 1 : this.maxResult);
            }
        }
        return setRelationEntities(ls, client, m);
    }

    /**
     * On executeUpdate.
     * 
     * @return zero
     */
    @Override
    protected int onExecuteUpdate()
    {
        EntityMetadata m = getEntityMetadata();

        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        String query = appMetadata.getQuery(getJPAQuery());

        boolean isNative = kunderaQuery.isNative();

        if (isNative)
        {
            ((CassandraClientBase) persistenceDelegeator.getClient(m)).executeQuery(m.getEntityClazz(), null,isNative,
                    query != null ? query : getJPAQuery());
        }
        else if (kunderaQuery.isDeleteUpdate())
        {
            // If query is not convertible to CQL, fetch and merge records usual
            // way, otherwise
            // convert to CQL and execute
            if (!isQueryConvertibleToCQL(kunderaQuery))
            {
                return onUpdateDeleteEvent();
            }
            else
            {
                query = null;
                if (kunderaQuery.isUpdateClause())
                {
                    query = createUpdateQuery(kunderaQuery);
                }
                else
                {
                    query = createDeleteQuery(kunderaQuery);
                }
                return ((CassandraClientBase) persistenceDelegeator.getClient(m)).executeUpdateDeleteQuery(query);
            }

        }
        return 0;
    }

    /**
     * Checks whether a given JPA DML query is convertible to CQL
     * 
     * @param m
     * @return
     */
    private boolean isQueryConvertibleToCQL(KunderaQuery kunderaQuery)
    {
        EntityMetadata m = kunderaQuery.getEntityMetadata();
        if (kunderaQuery.isUpdateClause() && m.isCounterColumnType())
            return false;

        List<String> opsNotAllowed = Arrays.asList(new String[] { ">", "<", ">=", "<=" });
        boolean result = false;
        if (!kunderaQuery.getFilterClauseQueue().isEmpty())
        {
            String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
            for (Object o : kunderaQuery.getFilterClauseQueue())
            {
                if (o instanceof FilterClause)
                {
                    FilterClause filterClause = (FilterClause) o;
                    if (!idColumn.equals(filterClause.getProperty())
                            || opsNotAllowed.contains(filterClause.getCondition()))
                    {
                        result = false;
                        break;
                    }
                    result = true;
                }
            }
        }
        return result;
    }

    /**
     * Gets the column list.
     * 
     * @param m
     *            the m
     * @param results
     *            the results
     * @return the column list
     */
    List<String> getColumnList(EntityMetadata m, String[] results, EmbeddableType compoundKey)
    {
        List<String> columns = new ArrayList<String>();
        if (results != null && results.length > 0)
        {
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entity = metaModel.entity(m.getEntityClazz());

            String keyFieldName = CassandraUtilities.getIdColumnName(m, externalProperties);
            for (int i = 1; i < results.length; i++)
            {
                if (results[i] != null)
                {
                    Attribute attribute = entity.getAttribute(results[i]);
                    if (attribute == null)
                    {
                        throw new QueryHandlerException("column type is null for: " + results);
                    }
                    else if (m.getIdAttribute().equals(attribute) && compoundKey != null)
                    {
                        Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();
                        for (Field field : fields)
                        {
                            if (!ReflectUtils.isTransientOrStatic(field))
                            {
                                Attribute compositeColumn = compoundKey.getAttribute(field.getName());
                                columns.add(((AbstractAttribute) compositeColumn).getJPAColumnName());
                            }
                        }
                    }
                    else if (m.getIdAttribute().equals(attribute) && compoundKey == null)
                    {
                        columns.add(keyFieldName);
                    }
                    else
                    {
                        columns.add(((AbstractAttribute) attribute).getJPAColumnName());
                    }
                }
            }
            return columns;
        }

        if (log.isInfoEnabled())
        {
            log.info("No record found, returning null.");
        }
        return null;
    }

    /**
     * Prepare index clause.
     * 
     * @param m
     *            the m
     * @param isQueryForInvertedIndex
     *            the is query for inverted index
     * @return the map
     */
    Map<Boolean, List<IndexClause>> prepareIndexClause(EntityMetadata m, boolean isQueryForInvertedIndex)
    {
        IndexClause indexClause = Selector.newIndexClause(Bytes.EMPTY, maxResult);
        List<IndexClause> clauses = new ArrayList<IndexClause>();
        List<IndexExpression> expr = new ArrayList<IndexExpression>();

        Map<Boolean, List<IndexClause>> idxClauses = new HashMap<Boolean, List<IndexClause>>(1);
        // check if id column are mixed with other columns or not?
        String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        boolean idPresent = false;

        if (log.isInfoEnabled())
        {
            log.info("Preparing index clause for query {}", getJPAQuery());
        }

        for (Object o : getKunderaQuery().getFilterClauseQueue())
        {
            if (o instanceof FilterClause)
            {
                FilterClause clause = ((FilterClause) o);
                // String fieldName = getColumnName(clause.getProperty());
                String fieldName = clause.getProperty();
                // in case id column matches with field name, set it for first
                // time.
                if (!idPresent && idColumn.equalsIgnoreCase(fieldName))
                {
                    idPresent = true;
                }

                String condition = clause.getCondition();
                Object value = clause.getValue();
                IndexOperator operator = getOperator(condition, idPresent);

                IndexExpression expression = Selector.newIndexExpression(fieldName, operator,
                        getBytesValue(fieldName, m, value));

                expr.add(expression);
            }
            else
            {
                // Case of AND and OR clause.
                String opr = o.toString();
                if (opr.equalsIgnoreCase("or"))
                {
                    log.error("Support for OR clause is not enabled within cassandra.");
                    throw new QueryHandlerException("Unsupported clause " + opr + " for cassandra.");
                }

            }
        }

        if (!StringUtils.isBlank(getKunderaQuery().getFilter()))
        {
            indexClause.setExpressions(expr);
            clauses.add(indexClause);
        }
        idxClauses.put(idPresent, clauses);

        return idxClauses;
    }

    /**
     * Gets the operator.
     * 
     * @param condition
     *            the condition
     * @param idPresent
     *            the id present
     * @return the operator
     */
    private IndexOperator getOperator(String condition, boolean idPresent)
    {
        if (/* !idPresent && */condition.equals("="))
        {
            return IndexOperator.EQ;
        }
        else if (/* !idPresent && */condition.equals(">"))
        {
            return IndexOperator.GT;
        }
        else if (/* !idPresent && */condition.equals("<"))
        {
            return IndexOperator.LT;
        }
        else if (condition.equals(">="))
        {
            return IndexOperator.GTE;
        }
        else if (condition.equals("<="))
        {
            return IndexOperator.LTE;
        }
        else
        {

            if (!idPresent)
            {
                throw new UnsupportedOperationException("Condition " + condition + " is not suported in  cassandra.");
            }
            else
            {
                throw new UnsupportedOperationException("Condition " + condition
                        + " is not suported for query on row key.");

            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        if (reader == null)
        {
            reader = new CassandraEntityReader(getLuceneQueryFromJPAQuery());
        }

        return reader;
    }

    /**
     * Returns bytes value for given value.
     * 
     * @param jpaFieldName
     *            field name.
     * @param m
     *            entity metadata
     * @param value
     *            value.
     * @return bytes value.
     */
    Bytes getBytesValue(String jpaFieldName, EntityMetadata m, Object value)
    {
        // Column idCol = m.getIdColumn();
        Attribute idCol = m.getIdAttribute();
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());
        Field f = null;
        boolean isId = false;
        if (((AbstractAttribute) idCol).getJPAColumnName().equals(jpaFieldName))
        {
            f = (Field) idCol.getJavaMember();
            isId = true;
        }
        else
        {
            if (jpaFieldName != null && jpaFieldName.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER) > 0)
            {
                String embeddedFieldName = jpaFieldName.substring(0,
                        jpaFieldName.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER));
                String columnFieldName = jpaFieldName.substring(
                        jpaFieldName.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER) + 1, jpaFieldName.length());

                Attribute embeddedAttr = entity.getAttribute(embeddedFieldName);
                try
                {
                    Class<?> embeddedClass = embeddedAttr.getJavaType();
                    if (Collection.class.isAssignableFrom(embeddedClass))
                    {
                        Class<?> genericClass = PropertyAccessorHelper.getGenericClass((Field) embeddedAttr
                                .getJavaMember());
                        f = genericClass.getDeclaredField(columnFieldName);
                    }
                    else
                    {
                        f = embeddedClass.getDeclaredField(columnFieldName);
                    }

                }
                catch (SecurityException e)
                {
                    log.error("Error while extrating " + jpaFieldName + ", Caused by: ", e);
                    throw new QueryHandlerException("Error while extrating " + jpaFieldName + ".");
                }
                catch (NoSuchFieldException e)
                {
                    log.error("Error while extrating " + jpaFieldName + ", Caused by: ", e);
                    throw new QueryHandlerException("Error while extrating " + jpaFieldName + ".");
                }

            }
            else
            {
                String discriminatorColumn = ((AbstractManagedType) entity).getDiscriminatorColumn();

                if (!jpaFieldName.equals(discriminatorColumn))
                {
                    String fieldName = m.getFieldName(jpaFieldName);

                    Attribute col = entity.getAttribute(fieldName);
                    // Column col = m.getColumn(jpaFieldName);
                    if (col == null)
                    {
                        throw new QueryHandlerException("column type is null for: " + jpaFieldName);
                    }
                    f = (Field) col.getJavaMember();
                }
            }

        }

        // need to do integer.parseInt..as value will be string in case of
        // create query.
        if (f != null && f.getType() != null)
        {
            return CassandraUtilities.toBytes(value, f);
        }/*
          * else if(f == null || value == null) {
          * log.error("Error while handling data type for " + jpaFieldName +
          * "."); throw new QueryHandlerException("Field type is null for " +
          * jpaFieldName + "."); }
          */
        else
        {
            // default is String type
            return CassandraUtilities.toBytes(value, String.class);
        }
    }

    /**
     * On query over composite columns.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param metaModel
     *            the meta model
     * @return the list
     */
    String onQueryOverCQL3(EntityMetadata m, Client client, MetamodelImpl metaModel, List<String> relations)
    {
        List<Object> result = new ArrayList<Object>();

        // select column will always be of entity field only!
        // where clause ordering

        Class compoundKeyClass = m.getIdAttribute().getBindableJavaType();
        EmbeddableType compoundKey = null;
        String idColumn;
        if (metaModel.isEmbeddable(compoundKeyClass))
        {
            compoundKey = metaModel.embeddable(compoundKeyClass);
            idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        }
        else
        {
            idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        }
        StringBuilder builder = new StringBuilder();

        boolean isPresent = false;
        List<String> columns = getColumnList(m, getKunderaQuery().getResult(), compoundKey);
        String selectQuery = columns != null && !columns.isEmpty() ? CQLTranslator.SELECT_QUERY
                : CQLTranslator.SELECTALL_QUERY;

        CQLTranslator translator = new CQLTranslator();

        selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), m.getTableName(), false).toString());

        builder = CassandraUtilities.appendColumns(builder, columns, selectQuery, translator);

        addWhereClause(builder);

        onCondition(m, metaModel, compoundKey, idColumn, builder, isPresent, translator);

        return builder.toString();
        // onLimit(builder);

        // result = ((CassandraClientBase)
        // client).executeQuery(builder.toString(), m.getEntityClazz(),
        // relations);
        // return result;
    }

    /**
     * Add provided max result limit.
     * 
     * @param builder
     *            string builder.
     */
    private void onLimit(StringBuilder builder)
    {
        builder.append(CQLTranslator.LIMIT);
        builder.append(isSingleResult ? 1 : this.maxResult);
    }

    /**
     * On condition.
     * 
     * @param m
     *            the m
     * @param metaModel
     *            the meta model
     * @param keyObj
     *            the compound key
     * @param idColumn
     *            the id column
     * @param builder
     *            the builder
     * @param isPresent
     *            the is present
     * @param translator
     *            the translator
     * @return true, if successful
     */
    private boolean onCondition(EntityMetadata m, MetamodelImpl metaModel, EmbeddableType keyObj, String idColumn,
            StringBuilder builder, boolean isPresent, CQLTranslator translator)
    {
        String partitionKey = null;
        boolean allowFiltering = false;
        for (Object o : getKunderaQuery().getFilterClauseQueue())
        {
            if (o instanceof FilterClause)
            {
                FilterClause clause = ((FilterClause) o);
                String fieldName = clause.getProperty();
                String condition = clause.getCondition();
                Object value = clause.getValue();

                // if compound key field is given in where clause.
                isPresent = true;

                if (keyObj != null && idColumn.equals(fieldName))
                {
                    Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();
                    // boolean useToken = true;
                    for (Field field : fields)
                    {
                        if (!ReflectUtils.isTransientOrStatic(field))
                        {
                            Attribute compositeColumn = keyObj.getAttribute(field.getName());
                            translator.buildWhereClause(builder,
                                    ((AbstractAttribute) compositeColumn).getJPAColumnName(), field, value);
                            if (partitionKey == null)
                            {
                                partitionKey = compositeColumn.getName();
                            }

                            if (!allowFiltering)
                            {
                                allowFiltering = fieldName.equals(partitionKey);
                            }
                        }
                    }
                }
                else if (keyObj != null && metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                        && StringUtils.contains(fieldName, '.'))
                {
                    // Means it is a case of composite column.
                    fieldName = fieldName.substring(fieldName.indexOf(".") + 1);
                    ((AbstractAttribute) keyObj.getAttribute(fieldName)).getJPAColumnName();
                    // compositeColumns.add(new
                    // BasicDBObject(compositeColumn,value));
                    // TODO for partition key in case of embedded key.
                    translator.buildWhereClause(builder,
                            ((AbstractAttribute) keyObj.getAttribute(fieldName)).getBindableJavaType(),
                            ((AbstractAttribute) keyObj.getAttribute(fieldName)).getJPAColumnName(), value, condition,
                            false);
                    if (partitionKey == null)
                    {
                        partitionKey = keyObj.getAttribute(fieldName).getName();
                    }
                    if (!allowFiltering)
                    {
                        allowFiltering = fieldName.equals(partitionKey);
                    }
                }
                else if (idColumn.equals(fieldName))
                {
                    translator.buildWhereClause(builder,
                            ((AbstractAttribute) m.getIdAttribute()).getBindableJavaType(),
                            CassandraUtilities.getIdColumnName(m, externalProperties), value, condition, true);
                }
                else
                {
                    Metamodel metamodel = KunderaMetadataManager.getMetamodel(m.getPersistenceUnit());
                    Attribute attribute = ((MetamodelImpl) metamodel).getEntityAttribute(m.getEntityClazz(),
                            m.getFieldName(fieldName));
                    translator.buildWhereClause(builder, ((AbstractAttribute) attribute).getBindableJavaType(),
                            fieldName, value, condition, false);
                    allowFiltering = true;
                }
            }
        }

        // String last AND clause.
        if (isPresent)
        {
            builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
        }

        List<SortOrdering> orders = getKunderaQuery().getOrdering();
        if (orders != null)
        {
            for (SortOrdering order : orders)
            {
                translator.buildOrderByClause(builder, order.getColumnName(), order.getOrder(), false);
                // as only one order clause will be available for a query
                break;
            }
        }

        if (allowFiltering)
        {
            onLimit(builder);
            builder.append(" ");
            translator.buildFilteringClause(builder);
        }
        else
        {
            onLimit(builder);
        }

        return isPresent;
    }

    /**
     * Adds the where clause.
     * 
     * @param builder
     *            the builder
     */
    void addWhereClause(StringBuilder builder)
    {
        if (!getKunderaQuery().getFilterClauseQueue().isEmpty())
        {
            builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        }
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Iterator iterate()
    {
        EntityMetadata m = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);
        externalProperties = ((CassandraClientBase) client).getExternalProperties();

        if (!MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
        {
            throw new UnsupportedOperationException("Scrolling over hbase is unsupported for lucene queries");
        }

        return new ResultIterator(this, m, persistenceDelegeator.getClient(m), this.getReader(),
                getFetchSize() != null ? getFetchSize() : this.maxResult);
    }

    void setRelationalEntities(List enhanceEntities, Client client, EntityMetadata m)
    {
        super.setRelationEntities(enhanceEntities, client, m);
    }

    @Override
    public Object getSingleResult()
    {
        // to fetch a single result form database.
        isSingleResult = true;
        List results = getResultList();
        isSingleResult = false;
        return results.isEmpty() ? results : results.get(0);
    }

    /**
     * Create Update CQL query from a given JPA query.
     * 
     * @param kunderaQuery
     * @return
     */
    public String createUpdateQuery(KunderaQuery kunderaQuery)
    {
        EntityMetadata metadata = kunderaQuery.getEntityMetadata();
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        CQLTranslator translator = new CQLTranslator();
        String update_Query = translator.UPDATE_QUERY;

        String tableName = metadata.getTableName();
        update_Query = StringUtils.replace(update_Query, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableName, false).toString());

        StringBuilder builder = new StringBuilder(update_Query);

        Object ttlColumns = ((CassandraClientBase) persistenceDelegeator.getClient(metadata)).getTtlValues().get(
                metadata.getTableName());
        if (ttlColumns != null && ttlColumns instanceof Integer)
        {
            int ttl = ((Integer) ttlColumns).intValue();
            if (ttl != 0)
            {
                builder.append(" USING TTL ");
                builder.append(ttl);
            }
        }

        builder.append(CQLTranslator.ADD_SET_CLAUSE);

        for (UpdateClause updateClause : kunderaQuery.getUpdateClauseQueue())
        {

            String property = updateClause.getProperty();

            String jpaColumnName = getColumnName(metadata, property);

            Object value = updateClause.getValue();

            translator.buildSetClause(metadata, builder, jpaColumnName, value);
        }
        builder.delete(builder.lastIndexOf(CQLTranslator.COMMA_STR), builder.length());
        builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        buildWhereClause(kunderaQuery, metadata, metaModel, translator, builder);

        return builder.toString();
    }

    /**
     * Create Delete query from a given JPA query
     * 
     * @param kunderaQuery
     * @return
     */
    public String createDeleteQuery(KunderaQuery kunderaQuery)
    {
        EntityMetadata metadata = kunderaQuery.getEntityMetadata();
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());
        CQLTranslator translator = new CQLTranslator();
        String delete_query = translator.DELETE_QUERY;

        String tableName = kunderaQuery.getEntityMetadata().getTableName();
        delete_query = StringUtils.replace(delete_query, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), tableName, false).toString());

        StringBuilder builder = new StringBuilder(delete_query);
        builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        buildWhereClause(kunderaQuery, metadata, metaModel, translator, builder);
        return builder.toString();
    }

    /**
     * Builds where Clause
     * 
     * @param kunderaQuery
     * @param metadata
     * @param metaModel
     * @param translator
     * @param builder
     */
    private void buildWhereClause(KunderaQuery kunderaQuery, EntityMetadata metadata, MetamodelImpl metaModel,
            CQLTranslator translator, StringBuilder builder)
    {
        for (Object clause : kunderaQuery.getFilterClauseQueue())
        {
            FilterClause filterClause = (FilterClause) clause;
            Field f = (Field) metaModel.entity(metadata.getEntityClazz())
                    .getAttribute(metadata.getFieldName(filterClause.getProperty())).getJavaMember();
            String jpaColumnName = getColumnName(metadata, filterClause.getProperty());

            if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType()))
            {
                Field[] fields = metadata.getIdAttribute().getBindableJavaType().getDeclaredFields();
                EmbeddableType compoundKey = metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
                for (Field field : fields)
                {
                    if (field != null && !Modifier.isStatic(field.getModifiers())
                            && !Modifier.isTransient(field.getModifiers())
                            && !field.isAnnotationPresent(Transient.class))
                    {
                        Attribute attribute = compoundKey.getAttribute(field.getName());
                        String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                        Object value = PropertyAccessorHelper.getObject(filterClause.getValue(), field);
                        // TODO
                        translator.buildWhereClause(builder, field.getType(), columnName, value,
                                filterClause.getCondition(), false);
                    }
                }
            }
            else
            {
                // TODO
                translator.buildWhereClause(builder, f.getType(), jpaColumnName, filterClause.getValue(),
                        filterClause.getCondition(), false);
            }

        }
        builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
    }

    /**
     * Gets column name for a given field name
     * 
     * @param metadata
     * @param metaModel
     * @param property
     * @return
     */
    private String getColumnName(EntityMetadata metadata, String property)
    {
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());
        String jpaColumnName = null;

        if (property.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
        {
            jpaColumnName = CassandraUtilities.getIdColumnName(metadata,
                    ((CassandraClientBase) persistenceDelegeator.getClient(metadata)).getExternalProperties());
        }
        else
        {
            jpaColumnName = ((AbstractAttribute) metaModel.getEntityAttribute(metadata.getEntityClazz(), property))
                    .getJPAColumnName();
        }
        return jpaColumnName;
    }

    boolean isNative()
    {
        return kunderaQuery.isNative();
    }
}