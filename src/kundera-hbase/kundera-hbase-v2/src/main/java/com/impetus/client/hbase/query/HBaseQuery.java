/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.InExpression;
import org.eclipse.persistence.jpa.jpql.parser.InputParameter;
import org.eclipse.persistence.jpa.jpql.parser.KeywordExpression;
import org.eclipse.persistence.jpa.jpql.parser.LikeExpression;
import org.eclipse.persistence.jpa.jpql.parser.LogicalExpression;
import org.eclipse.persistence.jpa.jpql.parser.NumericLiteral;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.parser.SelectStatement;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.StringLiteral;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.HBaseEntityReader;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQueryUtils;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class HBaseQuery.
 * 
 * @author Pragalbh Garg
 */
public class HBaseQuery extends QueryImpl
{

    /** the log used by this class. */
    private static Logger logger = LoggerFactory.getLogger(HBaseQuery.class);

    /**
     * Instantiates a new h base query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public HBaseQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
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
        return onQuery(m, client);
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
        // required in case of associated entities.
        List ls = onQuery(m, client);
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
        return new HBaseEntityReader(kunderaQuery, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        return onUpdateDeleteEvent();
    }

    /**
     * On query.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @return the list
     */
    private List onQuery(EntityMetadata m, Client client)
    {
        boolean useLuceneOrES = !MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata());
        QueryTranslator translator = new QueryTranslator();
        List<Map<String, Object>> columnsToOutput = translator.getColumnsToOutput(m, getKunderaQuery(), useLuceneOrES);
        translator.translate(getKunderaQuery(), m, useLuceneOrES);
        Filter filters = translator.getFilters();
        if (!translator.isWhereOrAggregationQuery() || !useLuceneOrES)
        {
            return ((HBaseClient) client).findData(m, null, translator.getStartRow(), translator.getEndRow(),
                    columnsToOutput, filters);
        }
        else
        {
            return populateUsingLucene(m, client, null, null);
        }
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

        boolean useLuceneOrES = !MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata());
        QueryTranslator translator = new QueryTranslator();
        translator.translate(getKunderaQuery(), m, useLuceneOrES);
        if (useLuceneOrES && translator.isWhereOrAggregationQuery())
        {
            throw new UnsupportedOperationException("Scrolling over hbase is unsupported for lucene or ES queries");
        }
        List<Map<String, Object>> columns = translator.getColumnsToOutput(m, getKunderaQuery(), useLuceneOrES);
        return new ResultIterator((HBaseClient) client, m, persistenceDelegeator,
                getFetchSize() != null ? getFetchSize() : this.maxResult, translator, columns);
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
        QueryTranslator translator = new QueryTranslator();
        translator.translate(getKunderaQuery(), m, true);
        List<Map<String, Object>> columns = translator.getColumnsToOutput(m, getKunderaQuery(), true);
        Object value = null;
        for (Object obj : getKunderaQuery().getFilterClauseQueue())
        {
            if (obj instanceof FilterClause)
            {
                value = ((FilterClause) obj).getValue().get(0);
            }
        }
        byte[] valueInBytes = HBaseUtils
                .getBytes(value, ((AbstractAttribute) m.getIdAttribute()).getBindableJavaType());
        return ((HBaseClient) client).findData(m, valueInBytes, null, null, columns, null);
    }

    /**
     * The Class QueryTranslator.
     */
    class QueryTranslator
    {
        /** The filter. */
        private Filter filter = null;

        /*
         * byte[] value for start row, in case of range query, else will contain
         * null.
         */
        /** The start row. */
        private byte[] startRow = null;

        /*
         * byte[] value for end row, in case of range query, else will contain
         * null.
         */
        /** The end row. */
        private byte[] endRow = null;

        /** The is where or aggregation. */
        private boolean isWhereOrAggregation = false;

        /**
         * Checks if is where or aggregation query.
         * 
         * @return true, if is where or aggregation query
         */
        public boolean isWhereOrAggregationQuery()
        {
            return isWhereOrAggregation;
        }

        /**
         * Gets the columns to output.
         * 
         * @param m
         *            the m
         * @param kunderaQuery
         *            the kundera query
         * @param useLuceneOrES
         *            the use lucene or es
         * @return the columns to output
         */
        private List<Map<String, Object>> getColumnsToOutput(EntityMetadata m, KunderaQuery kunderaQuery,
                Boolean useLuceneOrES)
        {
            if (kunderaQuery.isAggregated())
            {
                if (!useLuceneOrES)
                {
                    throw new KunderaException("Aggregations are not supported in this database. "
                            + "Please use Lucene or ES to create indexes if you want to do it");
                }
                else
                {
                    this.isWhereOrAggregation = true;
                }
            }
            if (kunderaQuery.isSelectStatement())
            {
                SelectStatement selectStatement = kunderaQuery.getSelectStatement();
                SelectClause selectClause = (SelectClause) selectStatement.getSelectClause();
                return readSelectClause(selectClause.getSelectExpression(), m, useLuceneOrES);
            }
            return new ArrayList();
        }

        /**
         * Read select clause.
         * 
         * @param selectExpression
         *            the select expression
         * @param m
         *            the m
         * @param useLuceneOrES
         *            the use lucene or es
         * @return the list
         */
        private List<Map<String, Object>> readSelectClause(Expression selectExpression, EntityMetadata m,
                Boolean useLuceneOrES)
        {
            List<Map<String, Object>> columnsToOutput = new ArrayList<Map<String, Object>>();
            if (StateFieldPathExpression.class.isAssignableFrom(selectExpression.getClass()))
            {
                addToOutputColumns(selectExpression, m, columnsToOutput);
            }
            else if (CollectionExpression.class.isAssignableFrom(selectExpression.getClass()))
            {
                CollectionExpression collExp = (CollectionExpression) selectExpression;
                ListIterator<Expression> itr = collExp.children().iterator();
                while (itr.hasNext())
                {
                    Expression exp = itr.next();
                    if (StateFieldPathExpression.class.isAssignableFrom(exp.getClass()))
                    {
                        addToOutputColumns(exp, m, columnsToOutput);
                    }
                }
            }
            return columnsToOutput;
        }

        /**
         * Adds the to output columns.
         * 
         * @param selectExpression
         *            the select expression
         * @param m
         *            the m
         * @param columnsToOutput
         *            the columns to output
         */
        private void addToOutputColumns(Expression selectExpression, EntityMetadata m,
                List<Map<String, Object>> columnsToOutput)
        {
            StateFieldPathExpression sfpExp = (StateFieldPathExpression) selectExpression;
            Map<String, Object> map = setFieldClazzAndColumnFamily(sfpExp, m);
            columnsToOutput.add(map);
        }

        /**
         * Translate.
         * 
         * @param query
         *            the query
         * @param m
         *            the m
         * @param useLuceneOrES
         *            the use lucene or es
         */
        void translate(KunderaQuery query, EntityMetadata m, Boolean useLuceneOrES)
        {
            FilterList filterList = new FilterList();
            // add filter for pagination
            filterList.addFilter(new PageFilter(getMaxResults()));
            // add filter for kundera auto id generation row
            filterList.addFilter(new RowFilter(CompareOp.NOT_EQUAL, new BinaryComparator(HBaseUtils.AUTO_ID_ROW
                    .getBytes())));
            // get filters from where clause
            Filter filterFromWhereClause = getFiltersFromWhereClause(query, m, useLuceneOrES);
            if (filterFromWhereClause != null)
            {
                filterList.addFilter(filterFromWhereClause);
            }
            // get filters for discriminator column
            Filter filterForDiscrCol = getFilterForDiscrColumn(m);
            if (filterForDiscrCol != null)
            {
                filterList.addFilter(filterForDiscrCol);
            }
            this.filter = filterList;
        }

        /**
         * Gets the filter for discr column.
         * 
         * @param m
         *            the m
         * @return the filter for discr column
         */
        private Filter getFilterForDiscrColumn(EntityMetadata m)
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            AbstractManagedType entity = (AbstractManagedType) metaModel.entity(m.getEntityClazz());
            if (entity.isInherited())
            {
                return new SingleColumnValueFilter(Bytes.toBytes(m.getTableName()), Bytes.toBytes(entity
                        .getDiscriminatorColumn()), CompareOp.EQUAL, Bytes.toBytes(entity.getDiscriminatorValue()));
            }
            return null;
        }

        /**
         * Sets the filters from where clause.
         * 
         * @param query
         *            the query
         * @param m
         *            the m
         * @param useLuceneOrES
         *            the use lucene or es
         * @return the filters from where clause
         */
        private Filter getFiltersFromWhereClause(KunderaQuery query, EntityMetadata m, Boolean useLuceneOrES)
        {
            String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
            WhereClause whereClause = KunderaQueryUtils.getWhereClause(query.getJpqlExpression());
            if (whereClause != null)
            {
                this.isWhereOrAggregation = true;
                if (!useLuceneOrES)
                    return traverse(whereClause.getConditionalExpression(), m, idColumn);
            }
            return null;
        }

        /**
         * Traverse.
         * 
         * @param expression
         *            the expression
         * @param m
         *            the m
         * @param idColumn
         *            the id column
         * @return the filter
         */
        private Filter traverse(Expression expression, EntityMetadata m, String idColumn)
        {
            Boolean isIdColumn = false;
            if (ComparisonExpression.class.isAssignableFrom(expression.getClass()))
            {
                return onComparisonExpression(expression, m, idColumn, isIdColumn);
            }
            else if (LogicalExpression.class.isAssignableFrom(expression.getClass()))
            {
                return onLogicalExpression(expression, m, idColumn);
            }
            else if (InExpression.class.isAssignableFrom(expression.getClass()))
            {
                return onInExpression(expression, m, idColumn, isIdColumn);
            }
            else if (LikeExpression.class.isAssignableFrom(expression.getClass()))
            {
                throw new KunderaException("LIKE query is currently not supported in HBase. "
                        + "Please use Lucene or ElasticSearch to create secondary indexes and use LIKE query");
            }
            return null;
        }

        /**
         * On comparison expression.
         * 
         * @param expression
         *            the expression
         * @param m
         *            the m
         * @param idColumn
         *            the id column
         * @param isIdColumn
         *            the is id column
         * @return the filter
         */
        private Filter onComparisonExpression(Expression expression, EntityMetadata m, String idColumn,
                Boolean isIdColumn)
        {
            ComparisonExpression compExp = (ComparisonExpression) expression;
            String condition = compExp.getIdentifier();
            StateFieldPathExpression sfpExp = (StateFieldPathExpression) compExp.getLeftExpression();
            Map<String, Object> map = setFieldClazzAndColumnFamily(sfpExp, m);
            Class fieldClazz = (Class) map.get(HBaseUtils.FIELD_CLAZZ);
            String colFamily = (String) map.get(HBaseUtils.COL_FAMILY);
            String columnName = (String) map.get(HBaseUtils.COL_NAME);
            isIdColumn = idColumn.equalsIgnoreCase(columnName);
            Object value = getValue(compExp.getRightExpression(), fieldClazz);
            if (!isEmbeddable(map))
            {
                byte[] valueInBytes = getValueInBytes(value, fieldClazz, isIdColumn, m);
                return createNewFilter(condition, Bytes.toBytes(colFamily), Bytes.toBytes(columnName), valueInBytes,
                        isIdColumn);
            }
            else
            {
                return createFilterForEmbeddables(condition, isIdColumn, m, fieldClazz, value);
            }
        }

        /**
         * Creates the filter for embeddables.
         * 
         * @param condition
         *            the condition
         * @param isIdColumn
         *            the is id column
         * @param m
         *            the m
         * @param fieldClazz
         *            the field clazz
         * @param value
         *            the value
         * @return the filter
         */
        private Filter createFilterForEmbeddables(String condition, Boolean isIdColumn, EntityMetadata m,
                Class fieldClazz, Object value)
        {
            if (isIdColumn)
            {
                String compositeKey = KunderaCoreUtils.prepareCompositeKey(m, value);
                return createNewFilter(condition, null, null, Bytes.toBytes(compositeKey), isIdColumn);
            }
            else
            {
                MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                        m.getPersistenceUnit());
                EmbeddableType embeddable = metaModel.embeddable(fieldClazz);
                Set<Attribute> attributes = embeddable.getAttributes();
                FilterList filterList = new FilterList();
                onEmbeddableField(value, attributes, condition, m, metaModel, filterList);
                return filterList;
            }
        }

        /**
         * On embeddable field.
         * 
         * @param embeddable
         *            the embeddable
         * @param attributes
         *            the attributes
         * @param condition
         *            the condition
         * @param m
         *            the m
         * @param metaModel
         *            the meta model
         * @param filterList
         *            the filter list
         */
        private void onEmbeddableField(Object embeddable, Set<Attribute> attributes, String condition,
                EntityMetadata m, MetamodelImpl metaModel, FilterList filterList)
        {
            for (Attribute attrib : attributes)
            {
                Class clazz = ((AbstractAttribute) attrib).getBindableJavaType();
                if (!metaModel.isEmbeddable(clazz))
                {
                    Object fieldValue = PropertyAccessorHelper.getObject(embeddable, (Field) attrib.getJavaMember());
                    String colFamily = ((AbstractAttribute) attrib).getTableName() != null ? ((AbstractAttribute) attrib)
                            .getTableName() : m.getTableName();
                    String columnName = ((AbstractAttribute) attrib).getJPAColumnName();
                    byte[] valueInBytes = getValueInBytes(fieldValue, clazz, false, m);
                    filterList.addFilter(createNewFilter(condition, Bytes.toBytes(colFamily),
                            Bytes.toBytes(columnName), valueInBytes, false));
                }
                else if (!attrib.isCollection())
                {
                    Set<Attribute> attribEmbeddables = metaModel.embeddable(
                            ((AbstractAttribute) attrib).getBindableJavaType()).getAttributes();
                    Object fieldValue = PropertyAccessorHelper.getObject(embeddable, (Field) attrib.getJavaMember());
                    onEmbeddableField(fieldValue, attribEmbeddables, condition, m, metaModel, filterList);
                }
                else
                {
                    throw new KunderaException(
                            "Either the field is collection or it contains a collection. This query is not supported in HBase");
                }
            }
        }

        /**
         * Checks if is embeddable.
         * 
         * @param map
         *            the map
         * @return true, if is embeddable
         */
        private boolean isEmbeddable(Map<String, Object> map)
        {
            return (boolean) map.get(HBaseUtils.IS_EMBEDDABLE);
        }

        /**
         * On logical expression.
         * 
         * @param expression
         *            the expression
         * @param m
         *            the m
         * @param idColumn
         *            the id column
         * @return the filter
         */
        private Filter onLogicalExpression(Expression expression, EntityMetadata m, String idColumn)
        {
            FilterList filterList = checkOperationAndReturnFilter((LogicalExpression) expression);
            Filter f = traverse(((LogicalExpression) expression).getLeftExpression(), m, idColumn);
            if (f != null)
            {
                filterList.addFilter(f);
            }
            f = traverse(((LogicalExpression) expression).getRightExpression(), m, idColumn);
            if (f != null)
            {
                filterList.addFilter(f);
            }
            return filterList.getFilters().size() == 0 ? null : filterList;
        }

        /**
         * On in expression.
         * 
         * @param expression
         *            the expression
         * @param m
         *            the m
         * @param idColumn
         *            the id column
         * @param isIdColumn
         *            the is id column
         * @return the filter
         */
        private Filter onInExpression(Expression expression, EntityMetadata m, String idColumn, Boolean isIdColumn)
        {
            InExpression inExp = (InExpression) expression;
            StateFieldPathExpression sfpExp = (StateFieldPathExpression) inExp.getExpression();
            Map<String, Object> map = setFieldClazzAndColumnFamily(sfpExp, m);
            Class fieldClazz = (Class) map.get(HBaseUtils.FIELD_CLAZZ);
            String colFamily = (String) map.get(HBaseUtils.COL_FAMILY);
            String columnName = (String) map.get(HBaseUtils.COL_NAME);
            isIdColumn = idColumn.equalsIgnoreCase(columnName);
            return onInClause(inExp, m, isIdColumn, Bytes.toBytes(colFamily), Bytes.toBytes(columnName), fieldClazz);
        }

        /**
         * On in clause.
         * 
         * @param values
         *            the values
         * @param m
         *            the m
         * @param isIdColumn
         *            the is id column
         * @param colFamily
         *            the col family
         * @param colName
         *            the col name
         * @param fieldClazz
         *            the field clazz
         * @return the filter
         */
        private Filter onInClause(Object values, EntityMetadata m, Boolean isIdColumn, byte[] colFamily,
                byte[] colName, Class fieldClazz)
        {
            InExpression inExp = (InExpression) values;
            Iterable listIterable;
            boolean isParameter = false;
            if (CollectionExpression.class.isAssignableFrom(inExp.getInItems().getClass()))
            {
                listIterable = ((CollectionExpression) inExp.getInItems()).children();
            }
            else
            {
                String param = ((InputParameter) inExp.getInItems()).getParameter();
                listIterable = (Iterable) getKunderaQuery().getParametersMap().get(param);
                isParameter = true;
            }
            Iterator itr = listIterable.iterator();
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            while (itr.hasNext())
            {
                Object value = isParameter ? itr.next() : getValue((Expression) itr.next(), fieldClazz);
                byte[] valueInBytes = getValueInBytes(value, fieldClazz, isIdColumn, m);
                Filter filter = !isIdColumn ? createNewFilter(HBaseUtils.EQUALS, colFamily, colName, valueInBytes,
                        isIdColumn) : new RowFilter(CompareOp.EQUAL, new BinaryComparator(valueInBytes));
                filterList.addFilter(filter);
            }
            return filterList.getFilters().size() == 0 ? null : filterList;
        }

        /**
         * Gets the value.
         * 
         * @param exp
         *            the exp
         * @param clazz
         *            the clazz
         * @return the value
         */
        private Object getValue(Expression exp, Class clazz)
        {
            if (StringLiteral.class.isAssignableFrom(exp.getClass()))
            {
                return ((StringLiteral) exp).getUnquotedText();

            }
            else if (NumericLiteral.class.isAssignableFrom(exp.getClass()))
            {
                return PropertyAccessorFactory.getPropertyAccessor(clazz).fromString(clazz,
                        ((NumericLiteral) exp).getText());
            }
            else if (InputParameter.class.isAssignableFrom(exp.getClass()))
            {
                InputParameter ip = (InputParameter) exp;
                return getKunderaQuery().getParametersMap().get(ip.getParameter());
            }
            else if (KeywordExpression.class.isAssignableFrom(exp.getClass()))
            {
                KeywordExpression keyWordExp = (KeywordExpression) exp;
                return PropertyAccessorFactory.getPropertyAccessor(clazz).fromString(clazz,
                        keyWordExp.getActualIdentifier());
            }
            else
            {
                logger.warn("Arithmetic expression is not supported in kundera-hbase");
                throw new KunderaException("Arithmetic expression is not supported in kundera-hbase");
            }
        }

        /**
         * Gets the value in bytes.
         * 
         * @param value
         *            the value
         * @param fieldClazz
         *            the field clazz
         * @param isIdColumn
         *            the is id column
         * @param m
         *            the m
         * @return the value in bytes
         */
        private byte[] getValueInBytes(Object value, Class fieldClazz, Boolean isIdColumn, EntityMetadata m)
        {
            AbstractAttribute idCol = (AbstractAttribute) m.getIdAttribute();
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            if (isIdColumn && metaModel.isEmbeddable(idCol.getBindableJavaType()))
            {
                Map<Attribute, List<Object>> columnValues = new HashMap<Attribute, List<Object>>();
                Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();
                EmbeddableType embeddable = metaModel.embeddable(m.getIdAttribute().getBindableJavaType());

                StringBuilder compositeKey = new StringBuilder();
                for (Field field : fields)
                {
                    if (!ReflectUtils.isTransientOrStatic(field))
                    {
                        AbstractAttribute attrib = (AbstractAttribute) embeddable.getAttribute(field.getName());
                        Object obj = PropertyAccessorHelper.getObject(value, field);
                        compositeKey.append(
                                PropertyAccessorHelper.fromSourceToTargetClass(String.class,
                                        attrib.getBindableJavaType(), obj)).append(HBaseUtils.COMP_KEY_DELIM);
                    }
                }
                compositeKey.delete(compositeKey.lastIndexOf(HBaseUtils.COMP_KEY_DELIM), compositeKey.length());
                value = compositeKey.toString();
                fieldClazz = String.class;
            }
            return HBaseUtils.getBytes(value, fieldClazz);
        }

        /**
         * Check operation and return filter.
         * 
         * @param expression
         *            the expression
         * @return the filter list
         */
        private FilterList checkOperationAndReturnFilter(LogicalExpression expression)
        {
            if (expression.getIdentifier().equals(HBaseUtils.AND))
            {
                return new FilterList(FilterList.Operator.MUST_PASS_ALL);
            }
            else if (expression.getIdentifier().equals(HBaseUtils.OR))
            {
                return new FilterList(FilterList.Operator.MUST_PASS_ONE);
            }

            return null;
        }

        /**
         * Creates the new filter.
         * 
         * @param condition
         *            the condition
         * @param colFamily
         *            the col family
         * @param colName
         *            the col name
         * @param value
         *            the value
         * @param isIdColumn
         *            the is id column
         * @return the filter
         */
        private Filter createNewFilter(String condition, byte[] colFamily, byte[] colName, byte[] value,
                Boolean isIdColumn)
        {
            CompareOp operator = HBaseUtils.getOperator(condition, isIdColumn, true);
            if (!isIdColumn)
            {
                return new SingleColumnValueFilter(colFamily, colName, operator, value);
            }
            else
            {
                switch (operator)
                {
                case GREATER_OR_EQUAL:
                    startRow = value;
                    break;
                case GREATER:
                    startRow = new byte[value.length + 1];
                    System.arraycopy(value, 0, startRow, 0, value.length);
                    break;
                case LESS:
                    endRow = value;
                    break;
                case LESS_OR_EQUAL:
                    endRow = new byte[value.length + 1];
                    System.arraycopy(value, 0, endRow, 0, value.length);
                    break;
                case EQUAL:
                    startRow = endRow = value;
                    break;
                case NOT_EQUAL:
                    return new RowFilter(CompareOp.NOT_EQUAL, new BinaryComparator(value));
                default:
                    throw new KunderaException("This comparison operator is currently not supported");
                }
                return null;
            }
        }

        /**
         * Sets the fieldclazz and colfamily.
         * 
         * @param expression
         *            the expression
         * @param m
         *            the m
         * @return the map
         */
        private Map<String, Object> setFieldClazzAndColumnFamily(Expression expression, EntityMetadata m)
        {
            StateFieldPathExpression sfpExp = (StateFieldPathExpression) expression;
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());

            EntityType entity = metaModel.entity(m.getEntityClazz());
            String discriminatorColumn = ((AbstractManagedType) entity).getDiscriminatorColumn();
            Class fieldClazz = String.class;
            String colFamily = m.getTableName();
            String colName = null;
            Map<String, Object> map = new HashMap<String, Object>();

            boolean isEmbeddable = false;
            int count = 1;
            AbstractAttribute attrib = (AbstractAttribute) entity.getAttribute(sfpExp.getPath(count++));
            isEmbeddable = metaModel.isEmbeddable(attrib.getBindableJavaType());
            while (sfpExp.pathSize() > count)
            {
                if (isEmbeddable)
                {
                    EmbeddableType embeddableType = metaModel.embeddable(attrib.getBindableJavaType());
                    attrib = (AbstractAttribute) embeddableType.getAttribute(sfpExp.getPath(count++));
                    isEmbeddable = metaModel.isEmbeddable(attrib.getBindableJavaType());
                }
            }
            if (!sfpExp.getPath(count - 1).equals(discriminatorColumn))
            {
                fieldClazz = attrib.getBindableJavaType();
                colFamily = attrib.getTableName() != null ? attrib.getTableName() : m.getTableName();
                colName = attrib.getJPAColumnName();
            }
            map.put(HBaseUtils.FIELD_CLAZZ, fieldClazz);
            map.put(HBaseUtils.COL_FAMILY, colFamily);
            map.put(HBaseUtils.COL_NAME, colName);
            map.put(HBaseUtils.IS_EMBEDDABLE, isEmbeddable);
            return map;
        }

        /**
         * Gets the filter.
         * 
         * @return the filter
         */
        Filter getFilters()
        {
            return this.filter;
        }

        /**
         * Gets the start row.
         * 
         * @return the start row
         */
        byte[] getStartRow()
        {
            return startRow;
        }

        /**
         * Gets the end row.
         * 
         * @return the end row
         */
        byte[] getEndRow()
        {
            return endRow;
        }

        /**
         * Checks if is range scan.
         * 
         * @return true, if is range scan
         */
        boolean isRangeScan()
        {
            return startRow != null || endRow != null;
        }
    }

}
