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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.client.cassandra.index.CassandraIndexHelper;
import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

/**
 * The Class CassQuery.
 * 
 * @author vivek.mishra
 */
public class CassQuery extends QueryImpl implements Query
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(CassQuery.class);

    /** The reader. */
    private EntityReader reader;

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
        log.debug("on populateEntities cassandra query");
        List<Object> result = null;
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        // if id attribute is embeddable, it is meant for CQL translation.
        // make it independent of embedded stuff and allow even to add non
        // composite into where clause and let cassandra complain for it.

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType()))
        {

            result = onQueryOverCompositeColumns(m, client, metaModel);
        }
        else
        {

            if (appMetadata.isNative(getJPAQuery()))
            {
                result = ((CassandraClientBase) client).executeQuery(appMetadata.getQuery(getJPAQuery()), m.getEntityClazz(), null);
            }
            else
            {
                if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit()))
                {
                    // Index in Inverted Index table if applicable
                    boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m);
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
                                    maxResult, getColumnList(m, getKunderaQuery().getResult(), null));
                        }
                        else
                        {
                            result = ((CassandraEntityReader) getReader()).handleFindByRange(m, client, result,
                                    ixClause, isRowKeyQuery, getColumnList(m, getKunderaQuery().getResult(), null));
                        }
                    }

                }
                else
                {
                    result = populateUsingLucene(m, client, result);
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
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<EnhanceEntity> ls = null;
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        if (appMetadata.isNative(getJPAQuery()))
        {
            ls = (List<EnhanceEntity>) ((CassandraClientBase) client).executeQuery(appMetadata.getQuery(getJPAQuery()), m.getEntityClazz(),
                    null);
        }
        else
        {
            // Index in Inverted Index table if applicable
            boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m);
            Map<Boolean, List<IndexClause>> ixClause = MetadataUtils.useSecondryIndex(m.getPersistenceUnit()) ? prepareIndexClause(
                    m, useInvertedIndex) : null;

            if (useInvertedIndex && !getKunderaQuery().getFilterClauseQueue().isEmpty())
            {
                ls = ((CassandraEntityReader) getReader()).readFromIndexTable(m, client, ixClause);
            }
            else
            {
                ((CassandraEntityReader) getReader()).setConditions(ixClause);
                ls = reader.populateRelation(m, client);
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
        if (KunderaMetadata.INSTANCE.getApplicationMetadata().isNative(getJPAQuery()))
        {
            ((CassandraClientBase) persistenceDelegeator.getClient(m)).executeQuery(KunderaMetadata.INSTANCE.getApplicationMetadata().getQuery(getJPAQuery()), m.getEntityClazz(),
                    null);
        }
        else if (kunderaQuery.isDeleteUpdate())
        {
            List result = getResultList();
            return result != null ? result.size() : 0;
            // throw new
            // QueryHandlerException("executeUpdate() is currently supported for native queries only");
        }

        return 0;
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
    private List<String> getColumnList(EntityMetadata m, String[] results, EmbeddableType compoundKey)
    {
        List<String> columns = new ArrayList<String>();
        if (results != null && results.length > 0)
        {
            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entity = metaModel.entity(m.getEntityClazz());
            for (int i = 1; i < results.length; i++)
            {
                if (results[i] != null)
                {
                    Attribute col = entity.getAttribute(results[i]);
                    if (col == null)
                    {
                        throw new QueryHandlerException("column type is null for: " + results);
                    }
                    else if (m.getIdAttribute().equals(col) && compoundKey != null)
                    {
                        Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();
                        for (Field field : fields)
                        {
                            Attribute compositeColumn = compoundKey.getAttribute(field.getName());
                            columns.add(((AbstractAttribute) compositeColumn).getJPAColumnName());
                        }
                    }
                    else
                    {
                        columns.add(((AbstractAttribute) col).getJPAColumnName());
                    }
                }
            }
            return columns;
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
    private Map<Boolean, List<IndexClause>> prepareIndexClause(EntityMetadata m, boolean isQueryForInvertedIndex)
    {
        IndexClause indexClause = Selector.newIndexClause(Bytes.EMPTY, maxResult);
        List<IndexClause> clauses = new ArrayList<IndexClause>();
        List<IndexExpression> expr = new ArrayList<IndexExpression>();

        Map<Boolean, List<IndexClause>> idxClauses = new HashMap<Boolean, List<IndexClause>>(1);
        // check if id column are mixed with other columns or not?
        String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        boolean idPresent = false;
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
                    log.error("Support for OR clause is not enabled with in cassandra");
                    throw new QueryHandlerException("unsupported clause " + opr + " for cassandra");
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
                throw new UnsupportedOperationException(" Condition " + condition + " is not suported in  cassandra!");
            }
            else
            {
                throw new UnsupportedOperationException(" Condition " + condition
                        + " is not suported for query on row key!");

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
    private Bytes getBytesValue(String jpaFieldName, EntityMetadata m, Object value)
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
                    log.error("Error while extrating " + jpaFieldName + ";Details:" + e.getMessage());
                    throw new QueryHandlerException("Error while extrating " + jpaFieldName);
                }
                catch (NoSuchFieldException e)
                {
                    log.error("Error while extrating " + jpaFieldName + ";Details:" + e.getMessage());
                    throw new QueryHandlerException("Error while extrating " + jpaFieldName);
                }

            }
            else
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

        // need to do integer.parseInt..as value will be string in case of
        // create query.
        if (f != null && f.getType() != null)
        {
            return CassandraUtilities.toBytes(value, f);
        }
        else
        {
            log.error("Error while handling data type for:" + jpaFieldName);
            throw new QueryHandlerException("field type is null for:" + jpaFieldName);
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
    private List<Object> onQueryOverCompositeColumns(EntityMetadata m, Client client, MetamodelImpl metaModel)
    {
        List<Object> result;
        // TODO: ensure ordering!

        // select column will always be of entity field only!
        // where clause ordering
        Class compoundKeyClass = m.getIdAttribute().getBindableJavaType();
        EmbeddableType compoundKey = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());
        String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();

        StringBuilder builder = new StringBuilder();

        boolean isPresent = false;
        List<String> columns = getColumnList(m, getKunderaQuery().getResult(), compoundKey);

        String selectQuery = columns != null && !columns.isEmpty() ? CQLTranslator.SELECT_QUERY
                : CQLTranslator.SELECTALL_QUERY;

        CQLTranslator translator = new CQLTranslator();

        selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMN_FAMILY,
                translator.ensureCase(new StringBuilder(), m.getTableName()).toString());

        builder = appendColumns(builder, columns, selectQuery, translator);

        addWhereClause(builder);

        isPresent = onCondition(m, metaModel, compoundKey, idColumn, builder, isPresent, translator);

        // String last AND clause.
        if (isPresent)
        {
            builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
        }

        result = ((CassandraClientBase) client).executeQuery(builder.toString(), m.getEntityClazz(), null);
        return result;
    }

    /**
     * On condition.
     * 
     * @param m
     *            the m
     * @param metaModel
     *            the meta model
     * @param compoundKey
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
    private boolean onCondition(EntityMetadata m, MetamodelImpl metaModel, EmbeddableType compoundKey, String idColumn,
            StringBuilder builder, boolean isPresent, CQLTranslator translator)
    {
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

                if (idColumn.equals(fieldName))
                {
                    Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();
                    for (Field field : fields)
                    {
                        Attribute compositeColumn = compoundKey.getAttribute(field.getName());
                        translator.buildWhereClause(builder, ((AbstractAttribute) compositeColumn).getJPAColumnName(),
                                field, value);
                    }

                }
                else if (metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                        && StringUtils.contains(fieldName, '.'))
                {
                    // Means it is a case of composite column.
                    fieldName = fieldName.substring(fieldName.indexOf(".") + 1);
                    // compositeColumns.add(new
                    // BasicDBObject(compositeColumn,value));
                    translator.buildWhereClause(builder, fieldName, value, condition);
                } else
                {
                    translator.buildWhereClause(builder, fieldName, value, condition);
                }
            }
        }
        return isPresent;
    }

    /**
     * Adds the where clause.
     * 
     * @param builder
     *            the builder
     */
    private void addWhereClause(StringBuilder builder)
    {
        if (!getKunderaQuery().getFilterClauseQueue().isEmpty())
        {
            builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        }
    }

    /**
     * Append columns.
     * 
     * @param builder
     *            the builder
     * @param columns
     *            the columns
     * @param selectQuery
     *            the select query
     * @param translator
     *            the translator
     */
    private StringBuilder appendColumns(StringBuilder builder, List<String> columns, String selectQuery, CQLTranslator translator)
    {
        if (columns != null)
        {
            for (String column : columns)
            {

                translator.appendColumnName(builder, column);
                builder.append(",");
            }
        }
        if (builder.lastIndexOf(",") != -1)
        {
            builder.deleteCharAt(builder.length() - 1);
            // selectQuery = StringUtils.replace(selectQuery,
            // CQLTranslator.COLUMN_FAMILY, builder.toString());
            selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMNS, builder.toString());
        }

        builder = new StringBuilder(selectQuery);
        return builder;

    }

}