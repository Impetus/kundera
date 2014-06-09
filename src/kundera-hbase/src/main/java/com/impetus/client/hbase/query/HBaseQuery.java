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
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

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

    /**
     * Constructor using fields.
     * 
     * @param query
     *            jpa query.
     * @param persistenceDelegator
     *            persistence delegator interface.
     */
    public HBaseQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator, final KunderaMetadata kunderaMetadata)
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
        List results = onQuery(m, client);
        return results;
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
        Map<Boolean, Filter> filter = translator.getFilter();
        if (translator.isFindById && (filter == null && columns == null))
        {
            List results = new ArrayList();

            Object output = client.find(m.getEntityClazz(), translator.rowKey);
            if (output != null)
            {
                results.add(output);
            }
            return results;
        }
        if (translator.isFindById && filter == null && columns != null)
        {
            return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.rowKey, translator.rowKey,
                    columns.toArray(new String[columns.size()]), null);
        }

        if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
        {
            if (filter == null && !translator.isFindById)
            {
                // means complete scan without where clause, scan all records.
                // findAll.
                if (translator.isRangeScan())
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(),
                            translator.getEndRow(), columns.toArray(new String[columns.size()]), null);
                }
                else
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, null, null,
                            columns.toArray(new String[columns.size()]), null);
                }
            }
            else
            {
                // means WHERE clause is present.
                Filter f = null;
                if (filter != null && filter.values() != null && !filter.values().isEmpty())
                {
                    f = filter.values().iterator().next();
                }
                if (translator.isRangeScan())
                {
                    return ((HBaseClient) client).findByRange(m.getEntityClazz(), m, translator.getStartRow(),
                            translator.getEndRow(), columns.toArray(new String[columns.size()]), f);
                }
                else
                {
                    // if range query. means query over id column. create range
                    // scan method.

                    // else setFilter to client and invoke new method. find by
                    // query if isFindById is false! else invoke findById
                    return ((HBaseClient) client).findByQuery(m.getEntityClazz(), m, f,
                            columns.toArray(new String[columns.size()]));
                }
            }
        }
        else
        {
            List results = null;
            return populateUsingLucene(m, client, results, null);
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
        /* filter list to hold collection for applied filters */
        private List<Filter> filterList;

        /* Returns true, if intended for id column */
        private boolean isIdColumn;

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

        /* is true, if query intended for row key equality. */
        private boolean isFindById;

        /* row key value. */
        byte[] rowKey;

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
                        if (opr.trim().equalsIgnoreCase("or"))
                        {
                            log.error("Support for OR clause is not enabled with in Hbase");
                            throw new QueryHandlerException("Unsupported clause " + opr + " for Hbase");
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
        Map<Boolean, Filter> getFilter()
        {
            if (filterList != null)
            {
                Map<Boolean, Filter> queryClause = new HashMap<Boolean, Filter>();
                queryClause.put(isIdColumn, new FilterList(filterList));
                return queryClause;
            }
            return null;
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
        private void onParseFilter(String condition, String name, Object value, boolean isIdColumn, EntityMetadata m, boolean useFilter)
        {
            CompareOp operator = HBaseUtils.getOperator(condition, isIdColumn,useFilter);
            byte[] valueInBytes = getBytes(name, m, value);

            if (!isIdColumn)
            {
                List<String> columns = null;
                if (new StringTokenizer(name, ".").countTokens() > 1)
                {
                    columns = getTranslatedColumns(m, new String[] { name }, 0);
                }

                if (columns != null && !columns.isEmpty())
                {
                    name = columns.get(0);
                }
                Filter f = new SingleColumnValueFilter(Bytes.toBytes(m.getTableName()), Bytes.toBytes(name), operator,
                        valueInBytes);
                addToFilter(f);
            }
            else
            {
                if (operator.equals(CompareOp.GREATER_OR_EQUAL) || operator.equals(CompareOp.GREATER))
                {
                    startRow = valueInBytes;
                }
                else if (operator.equals(CompareOp.LESS_OR_EQUAL) || operator.equals(CompareOp.LESS))
                {
                    endRow = valueInBytes;
                }
                else if (operator.equals(CompareOp.EQUAL))
                {
                    rowKey = getBytes(m.getIdAttribute().getName(), m, value);
                    endRow = null;
                    isFindById = true;
                }
            }
            this.isIdColumn = isIdColumn;
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

        /**
         * @return the isFindById
         */
        boolean isFindById()
        {
            return isFindById;
        }

        boolean isRangeScan()
        {
            return startRow != null || endRow != null && !isFindById;
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
        Attribute idCol = m.getIdAttribute();
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());
        Class fieldClazz = null;
        if (idCol.getName().equals(jpaFieldName))
        {
            Field f = (Field) idCol.getJavaMember();
            fieldClazz = f.getType();
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
}
