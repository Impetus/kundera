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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javassist.Modifier;

import javax.persistence.Embeddable;
import javax.persistence.Transient;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.eclipse.persistence.jpa.jpql.parser.CountFunction;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
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
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.KunderaQuery.SortOrdering;
import com.impetus.kundera.query.KunderaQuery.UpdateClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class CassQuery.
 * 
 * @author vivek.mishra
 * 
 *         Query implementation for Cassandra.
 */
@SuppressWarnings("unchecked")
public class CassQuery extends QueryImpl {

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(CassQuery.class);

    /** The reader. */
    private EntityReader reader;

    /** The external properties. */
    protected Map<String, Object> externalProperties;

    /**
     * Instantiates a new cass query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CassQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
        final KunderaMetadata kunderaMetadata) {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera .metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client) {
        if (log.isDebugEnabled()) {
            log.debug("Populating entities for Cassandra query {}.", getJPAQuery());
        }
        List<Object> result = new ArrayList<Object>();
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        externalProperties = ((CassandraClientBase) client).getExternalProperties();

        // if id attribute is embeddable, it is meant for CQL translation.
        // make it independent of embedded stuff and allow even to add non
        // composite into where clause and let cassandra complain for it.

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
            .getMetamodel(m != null ? m.getPersistenceUnit() : client.getPersistenceUnit());

        String query = appMetadata.getQuery(getJPAQuery());
        boolean isNative = kunderaQuery.isNative();
        if (!isNative && !MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata())) {
            result = populateUsingLucene(m, client, result, getKunderaQuery().getResult());
        }
        // change for embeddable
        else if (!isNative && ((CassandraClientBase) client).isCql3Enabled(m)
            && MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata())) {
            result = ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), null, isNative,
                onQueryOverCQL3(m, client, metaModel, null));
        } else {
            if (isNative) {
                // XXX
                if (!kunderaQuery.getBindParameters().isEmpty()) {
                    result = ((CassandraClientBase) client).executeQuery(null, null, isNative,
                        query != null ? query : getJPAQuery(), kunderaQuery.getBindParameters());
                } else {
                    result = ((CassandraClientBase) client).executeQuery(m != null ? m.getEntityClazz() : null, null,
                        isNative, query != null ? query : getJPAQuery());
                }
            } else {
                if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata())) {
                    // Index in Inverted Index table if applicable
                    boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m,
                        MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()));
                    Map<Boolean, List<IndexClause>> ixClause = prepareIndexClause(m, useInvertedIndex);
                    if (useInvertedIndex && !getKunderaQuery().getFilterClauseQueue().isEmpty()) {
                        result = (List) ((CassandraEntityReader) getReader()).readFromIndexTable(m, client, ixClause);
                    } else {
                        boolean isRowKeyQuery = ixClause.keySet().iterator().next();
                        if (!isRowKeyQuery) {
                            result = ((CassandraClientBase) client).find(ixClause.get(isRowKeyQuery), m, false, null,
                                isSingleResult ? 1 : this.maxResult,
                                getColumnList(m, metaModel, getKunderaQuery().getResult(), null));
                        } else {
                            result =
                                ((CassandraEntityReader) getReader()).handleFindByRange(m, client, result, ixClause,
                                    isRowKeyQuery, getColumnList(m, metaModel, getKunderaQuery().getResult(), null),
                                    isSingleResult ? 1 : this.maxResult);
                        }
                    }
                }

            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#findUsingLucene(com.impetus.kundera .metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    protected List findUsingLucene(EntityMetadata m, Client client) {
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());
        boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m,
            MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()));
        Map<Boolean, List<IndexClause>> ixClause = prepareIndexClause(m, useInvertedIndex);
        List<Object> result = new ArrayList<Object>();
        if (((CassandraClientBase) client).isCql3Enabled(m)) {
            result = ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), m.getRelationNames(), false,
                onQueryOverCQL3(m, client, metaModel, m.getRelationNames()));
        } else {
            result = ((CassandraEntityReader) getReader()).handleFindByRange(m, client, result, ixClause, true,
                getColumnList(m, metaModel, getKunderaQuery().getResult(), null), isSingleResult ? 1 : this.maxResult);
        }
        return result;
    }

    /**
     * (non-Javadoc).
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @return the list
     * @see com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus
     *      .kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client) {
        List<EnhanceEntity> ls = null;
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        externalProperties = ((CassandraClientBase) client).getExternalProperties();
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());

        String query = appMetadata.getQuery(getJPAQuery());
        boolean isNative = kunderaQuery.isNative();

        if (isNative) {
            ls = (List<EnhanceEntity>) ((CassandraClientBase) client).executeQuery(m.getEntityClazz(),
                m.getRelationNames(), isNative, query != null ? query : getJPAQuery());
        } else if (!isNative && ((CassandraClientBase) client).isCql3Enabled(m)) {
            // edited
            // check if lucene or indexer are enabled then populate
            if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata())) {
                ls = ((CassandraClientBase) client).executeQuery(m.getEntityClazz(), m.getRelationNames(), isNative,

                    onQueryOverCQL3(m, client, metaModel, m.getRelationNames()));
            } else {
                ls = populateUsingLucene(m, client, null, getKunderaQuery().getResult());
            }
        } else {
            // Index in Inverted Index table if applicable
            boolean useInvertedIndex = CassandraIndexHelper.isInvertedIndexingApplicable(m,
                MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()));
            Map<Boolean, List<IndexClause>> ixClause =
                MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata())
                    ? prepareIndexClause(m, useInvertedIndex) : null;

            if (useInvertedIndex && !getKunderaQuery().getFilterClauseQueue().isEmpty()) {
                ls = ((CassandraEntityReader) getReader()).readFromIndexTable(m, client, ixClause);
            } else {
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
    protected int onExecuteUpdate() {
        EntityMetadata m = getEntityMetadata();
        Client client = m != null ? persistenceDelegeator.getClient(m)
            : persistenceDelegeator.getClient(kunderaQuery.getPersistenceUnit());
        externalProperties = ((CassandraClientBase) client).getExternalProperties();
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        String query = appMetadata.getQuery(getJPAQuery());

        boolean isNative = kunderaQuery.isNative();

        if (isNative) {
            ((CassandraClientBase) client).executeQuery(m == null ? null : m.getEntityClazz(), null, isNative,
                query != null ? query : getJPAQuery());
        } else if (kunderaQuery.isDeleteUpdate()) {
            // If query is not convertible to CQL, fetch and merge records usual
            // way, otherwise
            // convert to CQL and execute
            if (!isQueryConvertibleToCQL(kunderaQuery)) {
                return onUpdateDeleteEvent();
            } else {
                query = null;
                if (kunderaQuery.isUpdateClause()) {
                    query = createUpdateQuery(kunderaQuery);
                } else {
                    query = createDeleteQuery(kunderaQuery);
                }
                return ((CassandraClientBase) client).executeUpdateDeleteQuery(query);
            }
        }
        return 0;
    }

    /**
     * Checks whether a given JPA DML query is convertible to CQL.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @return true, if is query convertible to cql
     */
    private boolean isQueryConvertibleToCQL(KunderaQuery kunderaQuery) {
        EntityMetadata m = kunderaQuery.getEntityMetadata();
        if (kunderaQuery.isUpdateClause() && m.isCounterColumnType())
            return false;

        List<String> opsNotAllowed = Arrays.asList(new String[] { ">", "<", ">=", "<=" });
        boolean result = false;
        if (!kunderaQuery.getFilterClauseQueue().isEmpty()) {
            String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
            for (Object o : kunderaQuery.getFilterClauseQueue()) {
                if (o instanceof FilterClause) {
                    FilterClause filterClause = (FilterClause) o;
                    if (!idColumn.equals(filterClause.getProperty())
                        || opsNotAllowed.contains(filterClause.getCondition())) {
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
     * @param metamodel
     *            the metamodel
     * @param results
     *            the results
     * @param compoundKey
     *            the compound key
     * @return the column list
     */
    List<String> getColumnList(EntityMetadata m, MetamodelImpl metamodel, String[] results,
        EmbeddableType compoundKey) {
        List<String> columns = new ArrayList<String>();
        if (results != null && results.length > 0) {
            MetamodelImpl metaModel =
                (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());
            EntityType entity = metaModel.entity(m.getEntityClazz());

            String keyFieldName = CassandraUtilities.getIdColumnName(kunderaMetadata, m, externalProperties,
                ((CassandraClientBase) persistenceDelegeator.getClient(m)).isCql3Enabled(m));
            for (int i = 1; i < results.length; i++) {
                if (results[i] != null) {
                    if (results[i].indexOf(".") > 0) {
                        String fieldName = results[i].substring(0, results[i].indexOf("."));
                        String embeddedFieldName =
                            results[i].substring(results[i].indexOf(".") + 1, results[i].length());
                        AbstractAttribute attribute = (AbstractAttribute) entity.getAttribute(fieldName);
                        EmbeddableType embeddable = metamodel.embeddable(attribute.getBindableJavaType());
                        Attribute embeddableAttribute = embeddable.getAttribute(embeddedFieldName);
                        columns.add(((AbstractAttribute) embeddableAttribute).getJPAColumnName());
                    } else {
                        Attribute attribute = entity.getAttribute(results[i]);
                        if (attribute == null) {
                            throw new QueryHandlerException("Column type is null for : " + results);
                        } else if (m.getIdAttribute().equals(attribute) && compoundKey != null) {
                            Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();
                            for (Field field : fields) {
                                addCompositeIdToColumns(metamodel, compoundKey, columns, field);
                            }
                        } else if (m.getIdAttribute().equals(attribute) && compoundKey == null) {
                            columns.add(keyFieldName);
                        } else {
                            columns.add(((AbstractAttribute) attribute).getJPAColumnName());
                        }
                    }
                }
            }
            return columns;
        }

        if (log.isInfoEnabled()) {
            log.info("No record found, returning null.");
        }
        return null;
    }

    /**
     * Adds the composite id to columns.
     * 
     * @param metamodel
     *            the metamodel
     * @param compoundKey
     *            the compound key
     * @param columns
     *            the columns
     * @param field
     *            the field
     */
    private void addCompositeIdToColumns(MetamodelImpl metamodel, EmbeddableType compoundKey, List<String> columns,
        Field field) {
        if (!ReflectUtils.isTransientOrStatic(field)) {
            Attribute compositeColumn = compoundKey.getAttribute(field.getName());
            if (compositeColumn.getJavaType().isAnnotationPresent(Embeddable.class)) {
                // partition key
                EmbeddableType partitionCol = metamodel.embeddable(compositeColumn.getJavaType());
                Set<Attribute> cols = partitionCol.getAttributes();
                for (Attribute col : cols) {
                    Field f = (Field) col.getJavaMember();
                    if (!ReflectUtils.isTransientOrStatic(f)) {
                        columns.add(((AbstractAttribute) col).getJPAColumnName());
                    }
                }
            } else {
                columns.add(((AbstractAttribute) compositeColumn).getJPAColumnName());
            }
        }
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
    Map<Boolean, List<IndexClause>> prepareIndexClause(EntityMetadata m, boolean isQueryForInvertedIndex) {
        IndexClause indexClause =
            new IndexClause(new ArrayList<IndexExpression>(), ByteBufferUtil.EMPTY_BYTE_BUFFER, maxResult);

        List<IndexClause> clauses = new ArrayList<IndexClause>();
        List<IndexExpression> expr = new ArrayList<IndexExpression>();

        Map<Boolean, List<IndexClause>> idxClauses = new HashMap<Boolean, List<IndexClause>>(1);
        // check if id column are mixed with other columns or not?
        String idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        boolean idPresent = false;

        if (log.isInfoEnabled()) {
            log.info("Preparing index clause for query {}", getJPAQuery());
        }

        for (Object o : getKunderaQuery().getFilterClauseQueue()) {
            if (o instanceof FilterClause) {
                FilterClause clause = ((FilterClause) o);
                String fieldName = clause.getProperty();
                // in case id column matches with field name, set it for first
                // time.
                if (!idPresent && idColumn.equalsIgnoreCase(fieldName)) {
                    idPresent = true;
                }

                String condition = clause.getCondition();
                List<Object> value = clause.getValue();
                if (value != null && value.size() > 1) {
                    log.error("IN clause is not enabled for thrift, use cql3.");
                    throw new QueryHandlerException("IN clause is not enabled for thrift, use cql3.");
                }
                IndexOperator operator = getOperator(condition, idPresent);

                IndexExpression expression = new IndexExpression(ByteBufferUtil.bytes(fieldName), operator,
                    getBytesValue(fieldName, m, value.get(0)));

                expr.add(expression);
            } else {
                // Case of AND and OR clause.
                String opr = o.toString();
                if (opr.equalsIgnoreCase("or")) {
                    log.error("Support for OR clause is not enabled within cassandra.");
                    throw new QueryHandlerException("Unsupported clause " + opr + " for cassandra.");
                }
            }
        }

        if (!StringUtils.isBlank(getKunderaQuery().getFilter())) {
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
    private IndexOperator getOperator(String condition, boolean idPresent) {
        if (/* !idPresent && */condition.equals("=")) {
            return IndexOperator.EQ;
        } else if (/* !idPresent && */condition.equals(">")) {
            return IndexOperator.GT;
        } else if (/* !idPresent && */condition.equals("<")) {
            return IndexOperator.LT;
        } else if (condition.equals(">=")) {
            return IndexOperator.GTE;
        } else if (condition.equals("<=")) {
            return IndexOperator.LTE;
        } else {
            if (!idPresent) {
                throw new UnsupportedOperationException("Condition " + condition + " is not suported in  cassandra.");
            } else {
                throw new UnsupportedOperationException(
                    "Condition " + condition + " is not suported for query on row key.");
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader() {
        if (reader == null) {
            reader = new CassandraEntityReader(kunderaQuery, kunderaMetadata);
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
    ByteBuffer getBytesValue(String jpaFieldName, EntityMetadata m, Object value) {
        Attribute idCol = m.getIdAttribute();
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());
        Field f = null;
        boolean isId = false;
        if (((AbstractAttribute) idCol).getJPAColumnName().equals(jpaFieldName)) {
            f = (Field) idCol.getJavaMember();
            isId = true;
        } else {
            if (jpaFieldName != null && jpaFieldName.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER) > 0) {
                String embeddedFieldName =
                    jpaFieldName.substring(0, jpaFieldName.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER));
                String columnFieldName = jpaFieldName.substring(
                    jpaFieldName.indexOf(Constants.INDEX_TABLE_ROW_KEY_DELIMITER) + 1, jpaFieldName.length());

                Attribute embeddedAttr = entity.getAttribute(embeddedFieldName);
                try {
                    Class<?> embeddedClass = embeddedAttr.getJavaType();
                    if (Collection.class.isAssignableFrom(embeddedClass)) {
                        Class<?> genericClass =
                            PropertyAccessorHelper.getGenericClass((Field) embeddedAttr.getJavaMember());
                        f = genericClass.getDeclaredField(columnFieldName);
                    } else {
                        f = embeddedClass.getDeclaredField(columnFieldName);
                    }

                } catch (SecurityException e) {
                    log.error("Error while extrating " + jpaFieldName + ", Caused by: ", e);
                    throw new QueryHandlerException("Error while extrating " + jpaFieldName + ".");
                } catch (NoSuchFieldException e) {
                    log.error("Error while extrating " + jpaFieldName + ", Caused by: ", e);
                    throw new QueryHandlerException("Error while extrating " + jpaFieldName + ".");
                }
            } else {
                String discriminatorColumn = ((AbstractManagedType) entity).getDiscriminatorColumn();

                if (!jpaFieldName.equals(discriminatorColumn)) {
                    String fieldName = m.getFieldName(jpaFieldName);

                    Attribute col = entity.getAttribute(fieldName);
                    if (col == null) {
                        throw new QueryHandlerException("column type is null for: " + jpaFieldName);
                    }
                    f = (Field) col.getJavaMember();
                }
            }
        }

        // need to do integer.parseInt..as value will be string in case of
        // create query.
        if (f != null && f.getType() != null) {
            return CassandraUtilities.toBytes(value, f);
        } else {
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
     * @param relations
     *            the relations
     * @return the list
     */
    public String onQueryOverCQL3(EntityMetadata m, Client client, MetamodelImpl metaModel, List<String> relations) {
        // select column will always be of entity field only!
        // where clause ordering

        Class compoundKeyClass = m.getIdAttribute().getBindableJavaType();
        EmbeddableType compoundKey = null;
        String idColumn;
        if (metaModel.isEmbeddable(compoundKeyClass)) {
            compoundKey = metaModel.embeddable(compoundKeyClass);
            idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        } else {
            idColumn = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        }
        StringBuilder builder = new StringBuilder();

        boolean isPresent = false;
        List<String> columns = getColumnList(m, metaModel, getKunderaQuery().getResult(), compoundKey);
        String selectQuery = setSelectQuery(columns);

        CQLTranslator translator = new CQLTranslator();

        selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), m.getTableName(), false).toString());

        builder = CassandraUtilities.appendColumns(builder, columns, selectQuery, translator);

        addWhereClause(builder);

        onCondition(m, metaModel, compoundKey, idColumn, builder, isPresent, translator, true);

        return builder.toString();
    }

    /**
     * Sets the select query.
     * 
     * @param columns
     *            the columns
     * @return the string
     */
    private String setSelectQuery(List<String> columns) {
        if (columns != null && !columns.isEmpty()) {
            return CQLTranslator.SELECT_QUERY;
        }
        if (kunderaQuery.isAggregated()) {
            Expression selectExpression =
                ((SelectClause) kunderaQuery.getSelectStatement().getSelectClause()).getSelectExpression();
            // create query depending on function
            if (selectExpression instanceof CountFunction) {
                return CQLTranslator.SELECT_COUNT_QUERY;
            }
        }
        return CQLTranslator.SELECTALL_QUERY;
    }

    /**
     * Add provided max result limit.
     * 
     * @param builder
     *            string builder.
     */
    private void onLimit(StringBuilder builder) {
        if (Integer.MAX_VALUE != maxResult) {
            builder.append(CQLTranslator.LIMIT);
            builder.append(isSingleResult ? 1 : this.maxResult);
        }
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
     * @param use
     *            the use
     * @return true, if successful
     */
    private boolean onCondition(EntityMetadata m, MetamodelImpl metaModel, EmbeddableType keyObj, String idColumn,
        StringBuilder builder, boolean isPresent, CQLTranslator translator, boolean use) {
        String partitionKey = null;
        boolean allowFiltering = false;
        for (Object o : getKunderaQuery().getFilterClauseQueue()) {
            if (o instanceof FilterClause) {
                FilterClause clause = ((FilterClause) o);
                String fieldName = clause.getProperty();
                String condition = clause.getCondition();
                List<Object> value = clause.getValue();
                boolean useInClause = condition.trim().equalsIgnoreCase("IN");

                // if compound key field is given in where clause.
                isPresent = true;

                if (keyObj != null && idColumn.equals(fieldName)) {
                    Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();

                    Map<Attribute, List<Object>> columnValues = new HashMap<Attribute, List<Object>>();

                    for (Field field : fields) {
                        if (!ReflectUtils.isTransientOrStatic(field)) {
                            extractCompositeKey(metaModel, keyObj, builder, translator, value, useInClause,
                                columnValues, field);
                        }
                    }

                    // Composite key always contains clusterKey.
                    allowFiltering = true;
                    if (useInClause) {
                        for (Attribute columnAttribute : columnValues.keySet()) {
                            isPresent = appendInClause(builder, translator, columnValues.get(columnAttribute),
                                ((AbstractAttribute) columnAttribute).getBindableJavaType(),
                                ((AbstractAttribute) columnAttribute).getJPAColumnName(), isPresent);
                        }
                    }
                } else if (keyObj != null && metaModel.isEmbeddable(m.getIdAttribute().getBindableJavaType())
                    && StringUtils.contains(fieldName, '.')) {
                    // Means it is a case of composite column.
                    isPresent = getCompoundKeyColumn(metaModel, keyObj, builder, isPresent, translator, fieldName,
                        condition, value, useInClause);

                    allowFiltering = true;
                } else if (idColumn.equals(fieldName)) {
                    // dont use token for equals query on id column (#856)
                    boolean useToken = use;
                    if (condition.equals("=")) {
                        useToken = false;
                    }
                    isPresent = buildWhereClause(builder, isPresent, translator, condition, value, useInClause,
                        ((AbstractAttribute) m.getIdAttribute()),
                        CassandraUtilities.getIdColumnName(kunderaMetadata, m, externalProperties,
                            ((CassandraClientBase) persistenceDelegeator.getClient(m)).isCql3Enabled(m)),
                        useToken);
                } else {
                    EntityType entity = metaModel.entity(m.getEntityClazz());
                    // Metamodel metamodel =
                    // KunderaMetadataManager.getMetamodel(kunderaMetadata,
                    // m.getPersistenceUnit());
                    String discriminatorColumn = ((AbstractManagedType) entity).getDiscriminatorColumn();
                    if (fieldName.equals(discriminatorColumn)) {
                        translator.buildWhereClause(builder, String.class, fieldName,
                            value.isEmpty() ? null : value.get(0), condition, false);
                        isPresent = true;

                    } else {
                        Metamodel metamodel =
                            KunderaMetadataManager.getMetamodel(kunderaMetadata, m.getPersistenceUnit());
                        Attribute attribute = ((MetamodelImpl) metamodel).getEntityAttribute(m.getEntityClazz(),
                            m.getFieldName(fieldName));

                        isPresent = buildWhereClause(builder, isPresent, translator, condition, value, useInClause,
                            ((AbstractAttribute) attribute), fieldName, false);
                    }

                    allowFiltering = true;
                }
            }
        }

        // Strip last AND clause.
        if (isPresent) {
            builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
        }

        // Append order by clause into query
        builder = appendOrderByClause(metaModel, m, keyObj, builder, translator);

        if (allowFiltering && use) {
            onLimit(builder);
            builder.append(" ");
            translator.buildFilteringClause(builder);
        } else if (use) {
            onLimit(builder);
        }

        return isPresent;
    }

    /**
     * Gets the compound key column.
     * 
     * @param metamodel
     *            the metamodel
     * @param keyObj
     *            the key obj
     * @param builder
     *            the builder
     * @param isPresent
     *            the is present
     * @param translator
     *            the translator
     * @param fieldName
     *            the field name
     * @param condition
     *            the condition
     * @param value
     *            the value
     * @param useInClause
     *            the use in clause
     * @return the compound key column
     */
    private boolean getCompoundKeyColumn(MetamodelImpl metamodel, EmbeddableType keyObj, StringBuilder builder,
        boolean isPresent, CQLTranslator translator, String fieldName, String condition, List<Object> value,
        boolean useInClause) {
        fieldName = fieldName.substring(fieldName.indexOf(".") + 1);

        // If partition key part age given in query, i.e. restriction on
        // id.compositekey.compositePartitionkey.partitionkeyColumn.
        if (fieldName.indexOf(".") > 0) {
            String compositePartitionkeyName = fieldName.substring(0, fieldName.indexOf("."));
            AbstractAttribute attribute = (AbstractAttribute) keyObj.getAttribute(compositePartitionkeyName);
            fieldName = fieldName.substring(fieldName.indexOf(".") + 1);

            EmbeddableType compositePartitionkey = metamodel.embeddable(attribute.getBindableJavaType());

            attribute = (AbstractAttribute) compositePartitionkey.getAttribute(fieldName);

            String columnName = attribute.getJPAColumnName();

            isPresent = buildWhereClause(builder, isPresent, translator, condition, value, useInClause, attribute,
                columnName, false);
        }
        // if composite partition key object is given in query, i.e. restriction
        // on id.compositekey.compositePartitionkey
        else if (metamodel.isEmbeddable(((AbstractAttribute) keyObj.getAttribute(fieldName)).getBindableJavaType())) {
            AbstractAttribute attribute = (AbstractAttribute) keyObj.getAttribute(fieldName);
            Set<Attribute> attributes = metamodel.embeddable(attribute.getBindableJavaType()).getAttributes();

            if (!useInClause) {
                // Iterating and appending each column of composite partition
                // key in query builder.
                for (Attribute nestedAttribute : attributes) {
                    String columnName = ((AbstractAttribute) nestedAttribute).getJPAColumnName();
                    Object valueObject = PropertyAccessorHelper.getObject(value.isEmpty() ? null : value.get(0),
                        (Field) nestedAttribute.getJavaMember());
                    translator.buildWhereClause(builder, nestedAttribute.getJavaType(), columnName, valueObject,
                        condition, false);
                }
            } else {
                throw new IllegalArgumentException("In clause is not supported on first part of partition key.");
            }
            isPresent = true;
        }
        // if Not a composite partition key,
        // id.compositekey.partitionkey/clusterKey.
        else {
            AbstractAttribute attribute = (AbstractAttribute) keyObj.getAttribute(fieldName);
            String columnName = attribute.getJPAColumnName();
            isPresent = buildWhereClause(builder, isPresent, translator, condition, value, useInClause, attribute,
                columnName, false);
        }
        return isPresent;
    }

    /**
     * Append order by clause.
     * 
     * @param metaModel
     *            the meta model
     * @param m
     *            the m
     * @param keyObj
     *            the key obj
     * @param builder
     *            the builder
     * @param translator
     *            the translator
     * @return the string builder
     */
    private StringBuilder appendOrderByClause(MetamodelImpl metaModel, EntityMetadata m, EmbeddableType keyObj,
        StringBuilder builder, CQLTranslator translator) {
        List<SortOrdering> orders = getKunderaQuery().getOrdering();

        if (orders != null) {
            builder.append(CQLTranslator.SPACE_STRING);
            builder.append(CQLTranslator.SORT_CLAUSE);

            for (SortOrdering order : orders) {
                String orderColumnName = order.getColumnName();

                orderColumnName = orderColumnName.substring(orderColumnName.indexOf(".") + 1, orderColumnName.length());

                String orderByColumnName;

                if (StringUtils.contains(orderColumnName, '.')) {
                    String propertyName = orderColumnName.substring(0, orderColumnName.indexOf("."));
                    Attribute embeddableAttribute = metaModel.getEntityAttribute(m.getEntityClazz(), propertyName);
                    EmbeddableType embeddableType =
                        metaModel.embeddable(((AbstractAttribute) embeddableAttribute).getBindableJavaType());
                    orderColumnName = orderColumnName.substring(orderColumnName.indexOf(".") + 1);
                    AbstractAttribute attribute = (AbstractAttribute) embeddableType.getAttribute(orderColumnName);
                    orderByColumnName = attribute.getJPAColumnName();

                } else {
                    Attribute attribute = metaModel.getEntityAttribute(m.getEntityClazz(), orderColumnName);
                    orderByColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
                }

                builder = translator.ensureCase(builder, orderByColumnName, false);
                builder.append(CQLTranslator.SPACE_STRING);
                builder.append(order.getOrder());
                builder.append(CQLTranslator.COMMA_STR);
            }

            if (!orders.isEmpty()) {
                builder.deleteCharAt(builder.lastIndexOf(CQLTranslator.COMMA_STR));
            }
        }
        return builder;
    }

    /**
     * Extract composite key.
     * 
     * @param metaModel
     *            the meta model
     * @param keyObj
     *            the key obj
     * @param builder
     *            the builder
     * @param translator
     *            the translator
     * @param value
     *            the value
     * @param useInClause
     *            the use in clause
     * @param columnValues
     *            the column values
     * @param field
     *            the field
     * @return true, if successful
     */
    private boolean extractCompositeKey(MetamodelImpl metaModel, EmbeddableType keyObj, StringBuilder builder,
        CQLTranslator translator, List<Object> value, boolean useInClause, Map<Attribute, List<Object>> columnValues,
        Field field) {
        Attribute compositeColumn = keyObj.getAttribute(field.getName());
        String jpaColumnName = ((AbstractAttribute) compositeColumn).getJPAColumnName();

        if (useInClause) {
            for (Object embeddedObject : value) {

                Object valueObject = PropertyAccessorHelper.getObject(embeddedObject, field);
                // Checking for composite partition key.
                if (metaModel.isEmbeddable(((AbstractAttribute) compositeColumn).getBindableJavaType())) {
                    Set<Attribute> attributes = metaModel
                        .embeddable(((AbstractAttribute) compositeColumn).getBindableJavaType()).getAttributes();

                    // Iterating over composite partition key columns.
                    for (Attribute nestedAttribute : attributes) {
                        List<Object> valueList = columnValues.get(compositeColumn);

                        if (valueList == null) {
                            valueList = new ArrayList<Object>();
                        }

                        Object obj =
                            PropertyAccessorHelper.getObject(valueObject, (Field) nestedAttribute.getJavaMember());
                        valueList.add(obj);
                        columnValues.put(nestedAttribute, valueList);
                    }
                } else {
                    List<Object> valueList = columnValues.get(compositeColumn);

                    if (valueList == null) {
                        valueList = new ArrayList<Object>();
                    }
                    valueList.add(valueObject);
                    columnValues.put(compositeColumn, valueList);
                }
            }
        } else {
            Object valueObject = PropertyAccessorHelper.getObject(value.isEmpty() ? null : value.get(0), field);

            // Checking for composite partition key.
            if (metaModel.isEmbeddable(((AbstractAttribute) compositeColumn).getBindableJavaType())) {
                Set<Attribute> attributes =
                    metaModel.embeddable(((AbstractAttribute) compositeColumn).getBindableJavaType()).getAttributes();

                // Iterating over composite partition key columns.
                for (Attribute nestedAttribute : attributes) {
                    String columnName = ((AbstractAttribute) nestedAttribute).getJPAColumnName();
                    Object obj = PropertyAccessorHelper.getObject(valueObject, (Field) nestedAttribute.getJavaMember());
                    translator.buildWhereClause(builder, nestedAttribute.getJavaType(), columnName, obj,
                        CQLTranslator.EQ_CLAUSE, false);
                }
                // returning true because builder has AND clause at end.
                return true;
            } else {
                translator.buildWhereClause(builder, field.getType(), jpaColumnName, valueObject,
                    CQLTranslator.EQ_CLAUSE, false);
                // returning true because builder has AND clause at end.
                return true;
            }
        }
        // returning false because builder does not have AND clause at end.
        return false;
    }

    /**
     * Builds the where clause.
     * 
     * @param builder
     *            the builder
     * @param isPresent
     *            the is present
     * @param translator
     *            the translator
     * @param condition
     *            the condition
     * @param value
     *            the value
     * @param useInClause
     *            the use in clause
     * @param idAttributeColumn
     *            the id attribute column
     * @param columnName
     *            the column name
     * @param useToken
     *            the use token
     * @return true, if successful
     */
    private boolean buildWhereClause(StringBuilder builder, boolean isPresent, CQLTranslator translator,
        String condition, List<Object> value, boolean useInClause, AbstractAttribute idAttributeColumn,
        String columnName, boolean useToken) {
        if (value.isEmpty()) {
            isPresent = appendIn(builder, translator, columnName);
            builder.append("( )");
            builder.append(" AND ");
        }
        // handle relations in Id
        else if (useInClause && value.size() > 1) {
            isPresent = appendInClause(builder, translator, value, idAttributeColumn.getBindableJavaType(), columnName,
                isPresent);
        } else {
            // TODO for partition key in case of embedded key.
            // idAttributeColumn.getBindableJavaType() was sending this class,
            // changed to getJavaType()
            translator.buildWhereClause(builder, ((Attribute) idAttributeColumn).getJavaType(), columnName,
                value.isEmpty() ? null : value.get(0), condition, useToken);
        }
        return isPresent;
    }

    /**
     * Append in.
     * 
     * @param builder
     *            the builder
     * @param translator
     *            the translator
     * @param columnName
     *            the column name
     * @return true, if successful
     */
    private boolean appendIn(StringBuilder builder, CQLTranslator translator, String columnName) {
        boolean isPresent;
        isPresent = true;
        translator.ensureCase(builder, columnName, false);
        builder.append(" IN ");
        return isPresent;
    }

    /**
     * Append in clause.
     * 
     * @param queryBuilder
     *            the query builder
     * @param translator
     *            the translator
     * @param value
     *            the value
     * @param fieldClazz
     *            the field clazz
     * @param columnName
     *            the column name
     * @param isPresent
     *            the is present
     * @return true, if successful
     */
    private boolean appendInClause(StringBuilder queryBuilder, CQLTranslator translator, List<Object> value,
        Class fieldClazz, String columnName, boolean isPresent) {
        isPresent = appendIn(queryBuilder, translator, columnName);
        queryBuilder.append("(");
        for (Object objectvalue : value) {
            translator.appendValue(queryBuilder, fieldClazz, objectvalue, isPresent, false);
            queryBuilder.append(", ");
        }

        queryBuilder.deleteCharAt(queryBuilder.lastIndexOf(", "));
        queryBuilder.append(") ");

        queryBuilder.append(" AND ");
        return isPresent;
    }

    /**
     * Adds the where clause.
     * 
     * @param builder
     *            the builder
     */
    void addWhereClause(StringBuilder builder) {
        if (!getKunderaQuery().getFilterClauseQueue().isEmpty()) {
            builder.append(CQLTranslator.ADD_WHERE_CLAUSE);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#close()
     */
    @Override
    public void close() {
        // Nothing to close.
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#iterate()
     */
    @Override
    public Iterator iterate() {
        if (kunderaQuery.isNative()) {
            throw new UnsupportedOperationException("Iteration not supported over native queries");
        }
        EntityMetadata m = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);
        externalProperties = ((CassandraClientBase) client).getExternalProperties();

        if (!MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata())) {
            throw new UnsupportedOperationException("Scrolling over cassandra is unsupported for lucene queries");
        }

        return new ResultIterator(this, m, persistenceDelegeator.getClient(m), this.getReader(),
            getFetchSize() != null ? getFetchSize() : this.maxResult, kunderaMetadata);
    }

    /**
     * Sets the relational entities.
     * 
     * @param enhanceEntities
     *            the enhance entities
     * @param client
     *            the client
     * @param m
     *            the m
     */
    public void setRelationalEntities(List enhanceEntities, Client client, EntityMetadata m) {
        super.setRelationEntities(enhanceEntities, client, m);
    }

    /**
     * Create Update CQL query from a given JPA query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @return the string
     */
    public String createUpdateQuery(KunderaQuery kunderaQuery) {
        EntityMetadata metadata = kunderaQuery.getEntityMetadata();
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());

        CQLTranslator translator = new CQLTranslator();
        String update_Query = translator.UPDATE_QUERY;

        String tableName = metadata.getTableName();
        update_Query = StringUtils.replace(update_Query, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), tableName, false).toString());

        StringBuilder builder = new StringBuilder(update_Query);

        Object ttlColumns = ((CassandraClientBase) persistenceDelegeator.getClient(metadata)).getTtlValues()
            .get(metadata.getTableName());

        if ((ttlColumns != null && ttlColumns instanceof Integer) || this.ttl != null) {
            int ttl = this.ttl != null ? this.ttl : ((Integer) ttlColumns).intValue();
            if (ttl != 0) {
                builder.append(" USING TTL ");
                builder.append(ttl);
                builder.append(" ");
            }
        }

        builder.append(CQLTranslator.ADD_SET_CLAUSE);

        for (UpdateClause updateClause : kunderaQuery.getUpdateClauseQueue()) {

            String property = updateClause.getProperty();

            String jpaColumnName = getColumnName(metadata, property);

            Object value = updateClause.getValue();

            translator.buildSetClause(metadata, builder, jpaColumnName, value);
        }
        builder.delete(builder.lastIndexOf(CQLTranslator.COMMA_STR), builder.length());
        builder.append(CQLTranslator.ADD_WHERE_CLAUSE);

        Class compoundKeyClass = metadata.getIdAttribute().getBindableJavaType();
        EmbeddableType compoundKey = null;
        String idColumn;
        if (metaModel.isEmbeddable(compoundKeyClass)) {
            compoundKey = metaModel.embeddable(compoundKeyClass);
            idColumn = ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName();
        } else {
            idColumn = ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName();
        }

        onCondition(metadata, metaModel, compoundKey, idColumn, builder, false, translator, false);

        return builder.toString();
    }

    /**
     * Create Delete query from a given JPA query.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @return the string
     */
    public String createDeleteQuery(KunderaQuery kunderaQuery) {
        EntityMetadata metadata = kunderaQuery.getEntityMetadata();
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
        CQLTranslator translator = new CQLTranslator();
        String delete_query = translator.DELETE_QUERY;

        String tableName = kunderaQuery.getEntityMetadata().getTableName();
        delete_query = StringUtils.replace(delete_query, CQLTranslator.COLUMN_FAMILY,
            translator.ensureCase(new StringBuilder(), tableName, false).toString());

        StringBuilder builder = new StringBuilder(delete_query);
        builder.append(CQLTranslator.ADD_WHERE_CLAUSE);

        Class compoundKeyClass = metadata.getIdAttribute().getBindableJavaType();
        EmbeddableType compoundKey = null;
        String idColumn;
        if (metaModel.isEmbeddable(compoundKeyClass)) {
            compoundKey = metaModel.embeddable(compoundKeyClass);
            idColumn = ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName();
        } else {
            idColumn = ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName();
        }

        onCondition(metadata, metaModel, compoundKey, idColumn, builder, false, translator, false);

        return builder.toString();
    }

    /**
     * Builds where Clause.
     * 
     * @param kunderaQuery
     *            the kundera query
     * @param metadata
     *            the metadata
     * @param metaModel
     *            the meta model
     * @param translator
     *            the translator
     * @param builder
     *            the builder
     */
    private void buildWhereClause(KunderaQuery kunderaQuery, EntityMetadata metadata, MetamodelImpl metaModel,
        CQLTranslator translator, StringBuilder builder) {
        for (Object clause : kunderaQuery.getFilterClauseQueue()) {
            FilterClause filterClause = (FilterClause) clause;
            Field f = (Field) metaModel.entity(metadata.getEntityClazz())
                .getAttribute(metadata.getFieldName(filterClause.getProperty())).getJavaMember();
            String jpaColumnName = getColumnName(metadata, filterClause.getProperty());

            if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType())) {
                Field[] fields = metadata.getIdAttribute().getBindableJavaType().getDeclaredFields();
                EmbeddableType compoundKey = metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
                for (Field field : fields) {
                    if (field != null && !Modifier.isStatic(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers()) && !field.isAnnotationPresent(Transient.class)) {
                        Attribute attribute = compoundKey.getAttribute(field.getName());
                        String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                        Object value = PropertyAccessorHelper.getObject(filterClause.getValue().get(0), field);
                        // TODO
                        translator.buildWhereClause(builder, field.getType(), columnName, value,
                            filterClause.getCondition(), false);
                    }
                }
            } else {
                translator.buildWhereClause(builder, f.getType(), jpaColumnName, filterClause.getValue().get(0),
                    filterClause.getCondition(), false);
            }
        }
        builder.delete(builder.lastIndexOf(CQLTranslator.AND_CLAUSE), builder.length());
    }

    /**
     * Gets column name for a given field name.
     * 
     * @param metadata
     *            the metadata
     * @param property
     *            the property
     * @return the column name
     */
    private String getColumnName(EntityMetadata metadata, String property) {
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit());
        String jpaColumnName = null;

        if (property.equals(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName())) {
            jpaColumnName = CassandraUtilities.getIdColumnName(kunderaMetadata, metadata,
                ((CassandraClientBase) persistenceDelegeator.getClient(metadata)).getExternalProperties(),
                ((CassandraClientBase) persistenceDelegeator.getClient(metadata)).isCql3Enabled(metadata));
        } else {
            jpaColumnName = ((AbstractAttribute) metaModel.getEntityAttribute(metadata.getEntityClazz(), property))
                .getJPAColumnName();
        }
        return jpaColumnName;
    }

    /**
     * Checks if is native.
     * 
     * @return true, if is native
     */
    boolean isNative() {
        return kunderaQuery.isNative();
    }

}
