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
package com.impetus.client.hbase.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.HBaseEntityReader;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Query implementation for HBase, translates JPQL into HBase Filters using
 * {@link QueryTranslator}.
 * 
 * @author vivek.mishra
 * 
 */
public class HBaseQuery extends QueryImpl
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseQuery.class);

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

        ClientMetadata clientMetadata = ((ClientBase) client).getClientMetadata();

        if (!MetadataUtils.useSecondryIndex(clientMetadata) && (clientMetadata.getIndexImplementor() != null))
        {
            return populateUsingLucene(m, client, null, kunderaQuery.getResult());
        }
        else
        {
            List results = onQuery(m, client);
            return results;
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
     * Parses and translates query into HBase filter and invokes client's method
     * to return list of entities.
     * 
     * @param m
     *            Entity metadata
     * @param client
     *            hbase client
     * @return list of entities.
     */
    private List onQuery(EntityMetadata m, Client client)
    {
        // Called only in case of standalone entity.
        QueryTranslator translator = new QueryTranslator();
        translator.translate(getKunderaQuery(), m, ((ClientBase) client).getClientMetadata());
        // start with 1 as first element is alias.
        List<String> columns = getTranslatedColumns(m, getKunderaQuery().getResult(), 1);
        Filter filter = translator.getFilter();
        if (translator.rowList != null && !translator.rowList.isEmpty())
        {
            return ((HBaseClient) client).findAll(m.getEntityClazz(), columns.toArray(new String[columns.size()]),
                    translator.getRowList());
        }
        if (!translator.isWhereQuery() && columns != null)
        {
            return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(), translator
                    .getEndRow(), columns.toArray(new String[columns.size()]), null, getKunderaQuery()
                    .getFilterClauseQueue());
        }

        if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
        {
            // if range query. means query over id column. create range
            // scan method.

            // else setFilter to client and invoke new method. find by
            // query if isFindById is false! else invoke findById
            if (translator.isWhereQuery() && !translator.isRangeScan())
            {
                return ((HBaseClient) client).findByQuery(m.getEntityClazz(), m, filter, getKunderaQuery()
                        .getFilterClauseQueue(), columns.toArray(new String[columns.size()]));
            }
            else
            {
                return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(), translator
                        .getEndRow(), columns.toArray(new String[columns.size()]), filter, getKunderaQuery()
                        .getFilterClauseQueue());
            }
        }
        else
        {
            return populateUsingLucene(m, client, null, null);
        }
    }

    /**
     * @param columns
     * @param m
     * @return
     */
    private List<String> getTranslatedColumns(EntityMetadata m, String[] columns, final int startWith)
    {
        List<String> translatedColumns = new ArrayList<String>();
        if (columns != null)
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());

            EntityType entity = metaModel.entity(m.getEntityClazz());
            for (int i = startWith; i < columns.length; i++)
            {
                if (columns[i] != null)
                {
                    String fieldName = null;
                    String embeddedFieldName = null;
                    // used string tokenizer to check for embedded column.
                    StringTokenizer stringTokenizer = new StringTokenizer(columns[i], ".");
                    // if need to select embedded columns
                    if (stringTokenizer.countTokens() > 1)
                    {
                        fieldName = stringTokenizer.nextToken();
                        embeddedFieldName = stringTokenizer.nextToken();
                        Attribute col = entity.getAttribute(fieldName); // get
                                                                        // embedded
                                                                        // column
                        EmbeddableType embeddableType = metaModel.embeddable(col.getJavaType()); // get
                                                                                                 // embeddable
                                                                                                 // type
                        Attribute attribute = embeddableType.getAttribute(embeddedFieldName);
                        translatedColumns.add(((AbstractAttribute) attribute).getJPAColumnName());
                    }
                    else
                    {
                        // For all columns
                        fieldName = columns[i];
                        Attribute col = entity.getAttribute(fieldName);
                        onEmbeddable(translatedColumns, metaModel, col,
                                metaModel.isEmbeddable(((AbstractAttribute) col).getBindableJavaType()));
                    }
                }
            }
        }
        return translatedColumns;
    }

    /**
     * @param translatedColumns
     * @param metaModel
     * @param col
     */
    private void onEmbeddable(List<String> translatedColumns, MetamodelImpl metaModel, Attribute col,
            boolean isEmbeddable)
    {
        if (isEmbeddable)
        {
            EmbeddableType embeddableType = metaModel.embeddable(col.getJavaType());

            Set<Attribute> attributes = embeddableType.getAttributes();

            for (Attribute attribute : attributes)
            {
                translatedColumns.add(((AbstractAttribute) attribute).getJPAColumnName());
            }
        }
        else
        {
            translatedColumns.add(((AbstractAttribute) col).getJPAColumnName());
        }
    }

    /**
     * Query translator to translate JPQL into HBase query definition(e.g.
     * Filter/Filterlist)
     * 
     * @author vivek.mishra
     * 
     */
    class QueryTranslator
    {

        private static final String CLOSE_BRACKET = ")";

        private static final String IN_CLAUSE = "IN";

        private static final String OR_OPERATOR = "OR";

        private static final String OPEN_BRACKET = "(";

        /* filter list to hold collection for applied filters */
        private List<Filter> filterList;

        /*
         * byte[] value for start row, in case of range query, else will contain
         * null.
         */
        private byte[] startRow;

        /*
         * byte[] value for end row, in case of range query, else will contain
         * null.
         */
        private byte[] endRow;

        /*
         * byte[] value for list of rows null.
         */
        private List rowList = new ArrayList();

        /*
         * boolean value for finding whether its OR or in query.
         */
        private boolean isORQuery;

        private boolean isWhereQuery;

        /**
         * @return
         */
        public boolean isORQuery()
        {
            return isORQuery;
        }

        /**
         * @return
         */
        public Object[] getRowList()
        {
            return rowList.toArray();
        }

        /**
         * @param rowList
         */
        public void setRowList(List rowList)
        {
            if (rowList == null)
            {
                rowList = new ArrayList<Filter>();
            }
            this.rowList = rowList;
        }

        /**
         * Translates kundera query into collection of to be applied HBase
         * filter/s.
         * 
         * @param query
         *            kundera query.
         * @param m
         *            entity's metadata.
         */
        void translate(KunderaQuery query, EntityMetadata m, ClientMetadata clientMetadata)
        {
            String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();

            boolean useFilter = MetadataUtils.useSecondryIndex(clientMetadata);
            for (Object obj : query.getFilterClauseQueue())
            {
                boolean isIdColumn = false;
                // parse for filter(e.g. where) clause.

                if (obj instanceof FilterClause)
                {
                    String condition = ((FilterClause) obj).getCondition();
                    String name = ((FilterClause) obj).getProperty();
                    Object value = ((FilterClause) obj).getValue().get(0);
                    if (idColumn.equalsIgnoreCase(name))
                    {
                        isIdColumn = true;
                    }
                    onParseFilter(condition, name, value, isIdColumn, m, useFilter);
                }
                else
                {
                    // Case of AND and OR clause.
                    String opr = obj.toString();
                    if (MetadataUtils.useSecondryIndex(clientMetadata))
                    {
                        if (opr.trim().equalsIgnoreCase(OR_OPERATOR))
                        {
                            this.isORQuery = true;
                            // log.error("Support for OR clause is not enabled with in Hbase");
                            // throw new
                            // QueryHandlerException("Unsupported clause " + opr
                            // + " for Hbase");
                        }
                    }
                }
            }
        }

        /**
         * Returns collection of parsed filter.
         * 
         * @return map.
         */
        Filter getFilter()
        {
            FilterList filters = new FilterList(new PageFilter(getMaxResults()));
            if (filterList != null)
            {
                this.setWhereQuery(true);
                if (this.isORQuery)
                {
                    filters.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, filterList));
                }
                else
                {
                    filters.addFilter(new FilterList(filterList));
                }
            }
            return filters;
        }

        /**
         * On parsing filter clause(e.g. WHERE clause).
         * 
         * @param condition
         *            condition
         * @param name
         *            column name.
         * @param value
         *            column value.
         * @param isIdColumn
         *            if it is an id column.
         * @param m
         *            entity metadata.
         */
        private void onParseFilter(String condition, String name, Object value, boolean isIdColumn, EntityMetadata m,
                boolean useFilter)
        {
            if (condition.trim().equalsIgnoreCase(IN_CLAUSE))
            {

                onInClause(name, value, m, isIdColumn);

            }
            else
            {
                SingleColumnFilterFactory factory = HBaseUtils.getOperator(condition, isIdColumn, useFilter);
                CompareOp operator = factory.getOperator();
                if (!isIdColumn)
                {
                    Filter f = createQualifierValueFilter(name, value, m, factory);
                    addToFilter(f);
                }
                else
                {
                    if (operator.equals(CompareOp.GREATER_OR_EQUAL) || operator.equals(CompareOp.GREATER))
                    {
                        byte[] valueInBytes = getBytes(name, m, value);
                        startRow = valueInBytes;
                    }
                    else if (operator.equals(CompareOp.LESS_OR_EQUAL) || operator.equals(CompareOp.LESS))
                    {
                        byte[] valueInBytes = getBytes(name, m, value);
                        endRow = valueInBytes;
                    }
                    else if (operator.equals(CompareOp.EQUAL))
                    {
                        startRow = endRow = getBytes(m.getIdAttribute().getName(), m, value);
                    }
                    else
                    {
                        throw new UnsupportedOperationException(" Condition " + condition
                                + " is not suported for query on row key!");
                    }
                }
            }
        }

        /**
         * @param name
         * @param value
         * @param m
         * @param factory
         * @return
         */
        private Filter createQualifierValueFilter(String name, Object value,
                EntityMetadata m, SingleColumnFilterFactory factory)
        {
            List<String> columns = null;
            byte[] valueInBytes = getBytes(name, m, value);
            if (new StringTokenizer(name, ".").countTokens() > 1)
            {
                columns = getTranslatedColumns(m, new String[] { name }, 0);
            }

            if (columns != null && !columns.isEmpty())
            {
                name = columns.get(0);
            }
            return factory.create(m.getTableName(), name, valueInBytes);
        }

        /**
         * @param name
         * @param value
         * @param m
         */
        private void onInClause(String name, Object value, EntityMetadata m, boolean isIdColumn)
        {
            List<String> columns = null;
            FilterList inClauseFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            if (new StringTokenizer(name, ".").countTokens() > 1)
            {
                columns = getTranslatedColumns(m, new String[] { name }, 0);
            }

            if (columns != null && !columns.isEmpty())
            {
                name = columns.get(0);
            }

            String itemValues = String.valueOf(value);
            itemValues = itemValues.startsWith(OPEN_BRACKET) && itemValues.endsWith(CLOSE_BRACKET) ? itemValues
                    .substring(1, itemValues.length() - 1) : itemValues;
            List<String> items = Arrays.asList(((String) itemValues).split("\\s*,\\s*"));

            for (String str : items)
            {
                str = str.trim();
                str = (str.startsWith("\"") && str.endsWith("\"")) || (str.startsWith("'") && str.endsWith("'")) ? str
                        .substring(1, str.length() - 1) : str;

                byte[] valueInBytes = getBytes(name, m, str);
                if (!isIdColumn)
                {
                    Filter f = new SingleColumnValueFilter(Bytes.toBytes(m.getTableName()), Bytes.toBytes(name),
                            CompareOp.EQUAL, valueInBytes);
                    inClauseFilterList.addFilter(f);

                }
                else
                {
                    rowList.add(valueInBytes);
                }

            }
            if (!inClauseFilterList.getFilters().isEmpty())
            {
                addToFilter(inClauseFilterList);
            }
        }

        /**
         * @return the startRow
         */
        byte[] getStartRow()
        {
            return startRow;
        }

        /**
         * @return the endRow
         */
        byte[] getEndRow()
        {
            return endRow;
        }

        boolean isRangeScan()
        {
            return startRow != null || endRow != null;
        }

        /**
         * @param f
         */
        private void addToFilter(Filter f)
        {
            if (filterList == null)
            {
                filterList = new ArrayList<Filter>();
            }
            filterList.add(f);
        }

        public boolean isWhereQuery()
        {
            return isWhereQuery;
        }

        public void setWhereQuery(boolean isWhereQuery)
        {
            this.isWhereQuery = isWhereQuery;
        }
    }

    /**
     * Returns bytes of value object.
     * 
     * @param jpaFieldName
     * @param m
     * @param value
     * @return
     */
    private byte[] getBytes(String jpaFieldName, EntityMetadata m, Object value)
    {
        AbstractAttribute idCol = (AbstractAttribute) m.getIdAttribute();
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());
        Class fieldClazz = null;
        if (idCol.getName().equals(jpaFieldName))
        {
            Field f = (Field) idCol.getJavaMember();

            if (metaModel.isEmbeddable(idCol.getBindableJavaType()))
            {
                fieldClazz = String.class;
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
                                        attrib.getBindableJavaType(), obj)).append("\001");
                    }
                }
                compositeKey.delete(compositeKey.lastIndexOf("\001"), compositeKey.length());
                value = compositeKey.toString();
            }
            else
            {
                fieldClazz = f.getType();
            }
        }
        else
        {
            StringTokenizer tokenizer = new StringTokenizer(jpaFieldName, ".");
            String embeddedFieldName = null;
            if (tokenizer.countTokens() > 1)
            {
                embeddedFieldName = tokenizer.nextToken();
                String fieldName = tokenizer.nextToken();
                Attribute embeddableAttribute = entity.getAttribute(embeddedFieldName);
                EmbeddableType embeddableType = metaModel.embeddable(embeddableAttribute.getJavaType());
                Attribute embeddedAttribute = embeddableType.getAttribute(fieldName);
                jpaFieldName = ((AbstractAttribute) embeddedAttribute).getJPAColumnName();
                fieldClazz = ((AbstractAttribute) embeddedAttribute).getBindableJavaType();
            }
            else
            {
                String discriminatorColumn = ((AbstractManagedType) entity).getDiscriminatorColumn();

                if (!jpaFieldName.equals(discriminatorColumn))
                {
                    String fieldName = m.getFieldName(jpaFieldName);
                    Attribute col = entity.getAttribute(fieldName);
                    fieldClazz = ((AbstractAttribute) col).getBindableJavaType();
                }
            }
        }

        if (fieldClazz != null)
        {
            return HBaseUtils.getBytes(value, fieldClazz);
        }
        else
        {
            // Treat default as UTF8-Type. { in case of discriminator column}
            return HBaseUtils.getBytes(value, String.class);
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

        if (!MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
        {
            throw new UnsupportedOperationException("Scrolling over hbase is unsupported for lucene queries");
        }
        QueryTranslator translator = new QueryTranslator();
        translator.translate(getKunderaQuery(), m, ((ClientBase) client).getClientMetadata());
        // start with 1 as first element is alias.
        List<String> columns = getTranslatedColumns(m, getKunderaQuery().getResult(), 1);

        return new ResultIterator((HBaseClient) client, m, persistenceDelegeator,
                getFetchSize() != null ? getFetchSize() : this.maxResult, translator, columns);
    }

    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {
        QueryTranslator translator = new QueryTranslator();
        translator.translate(getKunderaQuery(), m, ((ClientBase) client).getClientMetadata());
        List<String> columns = getTranslatedColumns(m, getKunderaQuery().getResult(), 1);

        return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(), translator
                .getEndRow(), columns.toArray(new String[columns.size()]), null, getKunderaQuery()
                .getFilterClauseQueue());
    }

}
