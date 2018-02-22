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
package com.impetus.kundera.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;

import javax.el.ExpressionFactory;
import javax.persistence.Parameter;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.eclipse.persistence.jpa.jpql.parser.DeleteStatement;
import org.eclipse.persistence.jpa.jpql.parser.EclipseLinkJPQLGrammar2_4;
import org.eclipse.persistence.jpa.jpql.parser.JPQLExpression;
import org.eclipse.persistence.jpa.jpql.parser.JPQLGrammar;
import org.eclipse.persistence.jpa.jpql.parser.SelectStatement;
import org.eclipse.persistence.jpa.jpql.parser.UpdateStatement;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class KunderaQuery.
 */
public class KunderaQuery {
    /** The Constant SINGLE_STRING_KEYWORDS. */
    public static final String[] SINGLE_STRING_KEYWORDS =
        { "SELECT", "UPDATE", "SET", "DELETE", "UNIQUE", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY" };

    /** The Constant INTER_CLAUSE_OPERATORS. */
    public static final String[] INTER_CLAUSE_OPERATORS = { "AND", "OR", "BETWEEN", "(", ")" };

    /** The Constant INTRA_CLAUSE_OPERATORS. */
    public static final String[] INTRA_CLAUSE_OPERATORS = { "=", "LIKE", "IN", ">", ">=", "<", "<=", "<>", "NOT IN" };

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KunderaQuery.class);

    /** The result. */
    private String[] result;

    /** The aggregation result. */
    private String[] aggregationResult;

    /** The from. */
    private String from;

    /** The filter. */
    private String filter;

    /** The ordering. */
    private String ordering;

    /** The entity name. */
    private String entityName;

    /** The entity alias. */
    private String entityAlias;

    /** The entity class. */
    private Class<?> entityClass;

    /** The sort orders. */
    private List<SortOrdering> sortOrders;

    /** The is aggregate. */
    private boolean isAggregate;

    /** Persistence Unit(s). */
    private String persistenceUnit;

    // contains a Queue of alternate FilterClause object and Logical Strings
    // (AND, OR etc.)
    /** The filters queue. */
    private Queue filtersQueue = new LinkedList();

    /** The is delete update. */
    private boolean isDeleteUpdate;

    /** The update clause queue. */
    private Queue<UpdateClause> updateClauseQueue = new LinkedList<UpdateClause>();

    /** The typed parameter. */
    private TypedParameter typedParameter;

    /** The parameters map. */
    private Map<String, Object> parametersMap = new HashMap<String, Object>();

    /** Bind parameters */
    private final List<BindParameter> bindParameters = new ArrayList<>();

    /** The is native query. */
    boolean isNativeQuery;

    /** The jpa query. */
    private String jpaQuery;

    /** The kundera metadata. */
    private final KunderaMetadata kunderaMetadata;

    /** The jpql expression. */
    private JPQLExpression jpqlExpression;

    /** The expression factory. */
    private ExpressionFactory expressionFactory;

    /** The select statement. */
    private SelectStatement selectStatement;

    /** The update statement. */
    private UpdateStatement updateStatement;

    /** The delete statement. */
    private DeleteStatement deleteStatement;

    /**
     * Sets the expression factory.
     * 
     * @param expressionFactory
     *            the expressionFactory to set
     */
    public void setExpressionFactory(ExpressionFactory expressionFactory) {
        this.expressionFactory = expressionFactory;
    }

    /**
     * Gets the jpql expression.
     * 
     * @return the jpqlExpression
     */
    public JPQLExpression getJpqlExpression() {
        return jpqlExpression;
    }

    /**
     * Instantiates a new kundera query.
     * 
     * @param jpaQuery
     *            the jpa query
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public KunderaQuery(final String jpaQuery, final KunderaMetadata kunderaMetadata) {
        this.jpaQuery = jpaQuery;
        this.kunderaMetadata = kunderaMetadata;
        initiateJPQLObject(jpaQuery);
    }

    /**
     * Initiate jpql object.
     * 
     * @param jpaQuery
     *            the jpa query
     */
    private void initiateJPQLObject(final String jpaQuery) {
        JPQLGrammar jpqlGrammar = EclipseLinkJPQLGrammar2_4.instance();
        this.jpqlExpression = new JPQLExpression(jpaQuery, jpqlGrammar, "ql_statement", true);
        setKunderaQueryTypeObject();
    }

    /**
     * Sets the kundera query type object.
     */
    private void setKunderaQueryTypeObject() {

        try {
            if (isSelectStatement()) {

                this.setSelectStatement((SelectStatement) (this.getJpqlExpression().getQueryStatement()));

            } else if (isUpdateStatement()) {

                this.setUpdateStatement((UpdateStatement) (this.getJpqlExpression().getQueryStatement()));

            } else if (isDeleteStatement()) {
                this.setDeleteStatement((DeleteStatement) (this.getJpqlExpression().getQueryStatement()));

            }
        } catch (ClassCastException cce) {
            throw new JPQLParseException("Bad query format : " + cce.getMessage());
        }

    }

    /**
     * Gets the select statement.
     * 
     * @return the selectStatement
     */
    public SelectStatement getSelectStatement() {
        return selectStatement;
    }

    /**
     * Sets the select statement.
     * 
     * @param selectStatement
     *            the selectStatement to set
     */
    public void setSelectStatement(SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    /**
     * Sets the update statement.
     * 
     * @param updateStatement
     *            the updateStatement to set
     */
    public void setUpdateStatement(UpdateStatement updateStatement) {
        this.updateStatement = updateStatement;
    }

    /**
     * Gets the update statement.
     * 
     * @return the updateStatement
     */
    public UpdateStatement getUpdateStatement() {
        return updateStatement;
    }

    /**
     * Gets the delete statement.
     * 
     * @return the deleteStatement
     */
    public DeleteStatement getDeleteStatement() {
        return deleteStatement;
    }

    /**
     * Sets the delete statement.
     * 
     * @param deleteStatement
     *            the deleteStatement to set
     */
    public void setDeleteStatement(DeleteStatement deleteStatement) {
        this.deleteStatement = deleteStatement;
    }

    /**
     * Checks if is select statement.
     * 
     * @return true, if is select statement
     */
    public boolean isSelectStatement() {
        return this.getJpqlExpression().getQueryStatement().getClass().isAssignableFrom(SelectStatement.class);

    }

    /**
     * Checks if is delete statement.
     * 
     * @return true, if is delete statement
     */
    public boolean isDeleteStatement() {
        return this.getJpqlExpression().getQueryStatement().getClass().isAssignableFrom(DeleteStatement.class);
    }

    /**
     * Checks if is update statement.
     * 
     * @return true, if is update statement
     */
    public boolean isUpdateStatement() {
        return this.getJpqlExpression().getQueryStatement().getClass().isAssignableFrom(UpdateStatement.class);
    }

    /**
     * Gets the expression factory.
     * 
     * @return the expressionFactory
     */
    public ExpressionFactory getExpressionFactory() {
        return expressionFactory;
    }

    /**
     * Sets the grouping.
     * 
     * @param groupingClause
     *            the new grouping
     */
    public void setGrouping(String groupingClause) {
    }

    /**
     * Sets the result.
     * 
     * @param result
     *            the new result
     */
    public final void setResult(String[] result) {
        this.result = result;
    }

    /**
     * Sets the aggregation result.
     * 
     * @param aggResult
     *            the new result
     */
    public final void setAggregationResult(String[] aggResult) {
        this.aggregationResult = aggResult;
    }

    /**
     * Gets the agg result.
     * 
     * @return Aggregation result set
     */
    public final String[] getAggResult() {
        return aggregationResult;
    }

    /**
     * Checks if is aggregated.
     * 
     * @return Query contains aggregation or not
     */
    public boolean isAggregated() {
        return isAggregate;
    }

    /**
     * Sets the aggregated.
     * 
     * @param isAggregated
     *            the new aggregated
     */
    public void setAggregated(boolean isAggregated) {
        this.isAggregate = isAggregated;
    }

    /**
     * Sets the from.
     * 
     * @param from
     *            the new from
     */
    public final void setFrom(String from) {
        this.from = from;
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    public final void setFilter(String filter) {
        this.filter = filter;
    }

    /**
     * Sets the ordering.
     * 
     * @param ordering
     *            the new ordering
     */
    public final void setOrdering(String ordering) {
        this.ordering = ordering;
        parseOrdering(this.ordering);
    }

    /**
     * Gets the filter.
     * 
     * @return the filter
     */
    public final String getFilter() {
        return filter;
    }

    /**
     * Gets the from.
     * 
     * @return the from
     */
    public final String getFrom() {
        return from;
    }

    /**
     * Gets the ordering.
     * 
     * @return the ordering
     */
    public final List<SortOrdering> getOrdering() {
        return sortOrders;
    }

    /**
     * Gets the result.
     * 
     * @return the result
     */
    public final String[] getResult() {
        return result;
    }

    /**
     * Gets the parameters map.
     * 
     * @return Map of query parameters.
     */
    public Map<String, Object> getParametersMap() {
        return parametersMap;
    }

    /**
     * Gets the bind parameters map.
     * 
     * @return Map of query parameters.
     */
    public List<BindParameter> getBindParameters() {
        return bindParameters;
    }

    /**
     * Method to check if required result is to get complete entity or a select scalar value.
     * 
     * @return true, if it result is for complete alias.
     * 
     */
    public final boolean isAliasOnly() {
        // TODO
        return result != null && (result[0].indexOf(".") == -1);
    }

    /**
     * Returns set of parameters.
     * 
     * @return jpaParameters
     */
    public Set<Parameter<?>> getParameters() {
        return typedParameter != null ? typedParameter.jpaParameters : null;
    }

    /**
     * Parameter is bound if it holds any value, else will return false.
     * 
     * @param param
     *            the param
     * @return true, if is bound
     */
    public boolean isBound(Parameter param) {
        return getClauseValue(param) != null;
    }

    /**
     * Returns clause value for supplied parameter.
     * 
     * @param paramString
     *            the param string
     * @return the clause value
     */
    public List<Object> getClauseValue(String paramString) {
        if (typedParameter != null && typedParameter.getParameters() != null) {
            List<FilterClause> clauses = typedParameter.getParameters().get(paramString);
            if (clauses != null) {
                return clauses.get(0).getValue();
            } else {
                throw new IllegalArgumentException("parameter is not a parameter of the query");
            }
        }

        logger.error("Parameter {} is not a parameter of the query.", paramString);
        throw new IllegalArgumentException("Parameter is not a parameter of the query.");
    }

    /**
     * Returns specific clause value.
     * 
     * @param param
     *            parameter
     * 
     * @return clause value.
     */
    public List<Object> getClauseValue(Parameter param) {
        Parameter match = null;
        if (typedParameter != null && typedParameter.jpaParameters != null) {
            for (Parameter p : typedParameter.jpaParameters) {
                if (p.equals(param)) {
                    match = p;
                    if (typedParameter.getType().equals(Type.NAMED)) {
                        List<FilterClause> clauses = typedParameter.getParameters().get(":" + p.getName());
                        if (clauses != null) {
                            return clauses.get(0).getValue();
                        }
                    } else {
                        List<FilterClause> clauses = typedParameter.getParameters().get("?" + p.getPosition());
                        if (clauses != null) {
                            return clauses.get(0).getValue();
                        } else {
                            UpdateClause updateClause = typedParameter.getUpdateParameters().get("?" + p.getPosition());
                            if (updateClause != null) {
                                List<Object> value = new ArrayList<Object>();
                                value.add(updateClause.getValue());
                                return value;
                            }

                        }
                    }
                    break;
                }
            }
            if (match == null) {
                throw new IllegalArgumentException("parameter is not a parameter of the query");
            }
        }

        logger.error("parameter{} is not a parameter of the query", param);
        throw new IllegalArgumentException("parameter is not a parameter of the query");
    }

    // must be executed after parse(). it verifies and populated the query
    // predicates.
    /**
     * Post parsing init.
     */
    protected void postParsingInit() {
        initEntityClass();
        initFilter();
        initUpdateClause();
    }

    /**
     * Inits the update clause.
     */
    private void initUpdateClause() {
        for (UpdateClause updateClause : updateClauseQueue) {

            onTypedParameter(updateClause.getValue(), updateClause, updateClause.getProperty().trim());
        }

    }

    /**
     * Inits the entity class.
     */
    private void initEntityClass() {
        if (from == null) {
            throw new JPQLParseException("Bad query format FROM clause is mandatory for SELECT queries");
        }
        String fromArray[] = from.split(" ");

        if (!this.isDeleteUpdate) {
            if (fromArray.length == 3 && fromArray[1].equalsIgnoreCase("as")) {
                fromArray = new String[] { fromArray[0], fromArray[2] };
            }

            if (fromArray.length != 2) {
                throw new JPQLParseException("Bad query format: " + from
                    + ". Identification variable is mandatory in FROM clause for SELECT queries");
            }

            // TODO
            StringTokenizer tokenizer = new StringTokenizer(result[0], ",");
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (!StringUtils.containsAny(fromArray[1] + ".", token)) {
                    throw new QueryHandlerException("bad query format with invalid alias:" + token);
                }
            }
        }

        this.entityName = fromArray[0];
        if (fromArray.length == 2)
            this.entityAlias = fromArray[1];

        persistenceUnit = kunderaMetadata.getApplicationMetadata().getMappedPersistenceUnit(entityName);

        // Get specific metamodel.
        MetamodelImpl model = getMetamodel(persistenceUnit);

        if (model != null) {
            entityClass = model.getEntityClass(entityName);
        }

        if (null == entityClass) {
            logger.error(
                "No entity {} found, please verify it is properly annotated with @Entity and not a mapped Super class",
                entityName);
            throw new QueryHandlerException("No entity found by the name: " + entityName);
        }

        EntityMetadata metadata = model.getEntityMetadata(entityClass);

        if (metadata != null && !metadata.isIndexable()) {
            throw new QueryHandlerException(entityClass + " is not indexed. Not possible to run a query on it."
                + " Check whether it was properly annotated for indexing.");
        }
    }

    /**
     * Inits the filter.
     */
    private void initFilter() {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        Metamodel metaModel = kunderaMetadata.getApplicationMetadata().getMetamodel(getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityClass);

        if (null == filter) {
            List<String> clauses = new ArrayList<String>();
            addDiscriminatorClause(clauses, entityType);
            return;
        }
        WhereClause whereClause = KunderaQueryUtils.getWhereClause(getJpqlExpression());

        KunderaQueryUtils.traverse(whereClause.getConditionalExpression(), metadata, kunderaMetadata, this, false);

        for (Object filterClause : filtersQueue) {

            if (!(filterClause instanceof String)) {
                onTypedParameter(((FilterClause) filterClause));
            }

        }

        addDiscriminatorClause(null, entityType);
    }

    /**
     * Adds the discriminator clause.
     * 
     * @param clauses
     *            the clauses
     * @param entityType
     *            the entity type
     */
    private void addDiscriminatorClause(List<String> clauses, EntityType entityType) {
        if (((AbstractManagedType) entityType).isInherited()) {
            String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
            String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

            if (discrColumn != null && discrValue != null) {
                if (clauses != null && !clauses.isEmpty()) {
                    filtersQueue.add("AND");
                }

                FilterClause filterClause = new FilterClause(discrColumn, "=", discrValue, discrColumn);
                filtersQueue.add(filterClause);
            }
        }
    }

    /**
     * Depending upon filter value, if it starts with ":" then it is NAMED parameter, else if starts with "?", it will
     * be INDEXED parameter.
     * 
     * @param value
     *            the value
     * @param updateClause
     *            the update clause
     * @param fieldName
     *            the field name
     */
    private void onTypedParameter(Object value, UpdateClause updateClause, String fieldName) {
        String token = value.toString();
        if (token != null && token.startsWith(":")) {
            addTypedParameter(Type.NAMED, token, updateClause);
            filterJPAParameterInfo(Type.NAMED, token.substring(1), fieldName);
        } else if (token != null && token.startsWith("?")) {
            addTypedParameter(Type.INDEXED, token, updateClause);
            filterJPAParameterInfo(Type.INDEXED, token.substring(1), fieldName);
        }
    }

    /**
     * Depending upon filter value, if it starts with ":" then it is NAMED parameter, else if starts with "?", it will
     * be INDEXED parameter.
     * 
     * @param filterClause
     *            filter clauses.
     */
    private void onTypedParameter(FilterClause filterClause) {

        if (filterClause.value != null && filterClause.value.get(0) == null) {
            return;
        }

        if (filterClause.value != null && filterClause.value.get(0).toString().startsWith(":")) {
            addTypedParameter(Type.NAMED, filterClause.value.get(0).toString(), filterClause);
            filterJPAParameterInfo(Type.NAMED, filterClause.value.get(0).toString().substring(1),
                filterClause.fieldName);
        } else if (filterClause.value.toString() != null && filterClause.value.get(0).toString().startsWith("?")) {
            addTypedParameter(Type.INDEXED, filterClause.value.get(0).toString(), filterClause);
            filterJPAParameterInfo(Type.INDEXED, filterClause.value.get(0).toString().substring(1),
                filterClause.fieldName);
        }
    }

    /**
     * Adds typed parameter to {@link TypedParameter}.
     * 
     * @param type
     *            type of parameter(e.g. NAMED/INDEXED)
     * @param parameter
     *            parameter name.
     * @param clause
     *            filter clause.
     */
    private void addTypedParameter(Type type, String parameter, FilterClause clause) {
        if (typedParameter == null) {
            typedParameter = new TypedParameter(type);
        }

        if (typedParameter.getType().equals(type)) {
            typedParameter.addParameters(parameter, clause);
        } else {
            logger.warn("Invalid type provided, it can either be name or indexes!");
        }
    }

    /**
     * Adds typed parameter to {@link TypedParameter}.
     * 
     * @param type
     *            type of parameter(e.g. NAMED/INDEXED)
     * @param parameter
     *            parameter name.
     * @param clause
     *            filter clause.
     */
    private void addTypedParameter(Type type, String parameter, UpdateClause clause) {
        if (type != null) {
            if (typedParameter == null) {
                typedParameter = new TypedParameter(type);
            }

            if (typedParameter.getType().equals(type)) {
                typedParameter.addParameters(parameter, clause);
            } else {
                logger.warn("Invalid type provided, it can either be name or indexes!");
            }
        }
    }

    /**
     * Filter jpa parameter info.
     * 
     * @param type
     *            the type
     * @param name
     *            the name
     * @param fieldName
     *            the field name
     */
    private void filterJPAParameterInfo(Type type, String name, String fieldName) {
        String attributeName = getAttributeName(fieldName);
        Attribute entityAttribute =
            ((MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(persistenceUnit))
                .getEntityAttribute(entityClass, attributeName);
        Class fieldType = entityAttribute.getJavaType();

        if (type.equals(Type.INDEXED)) {
            typedParameter.addJPAParameter(new JPAParameter(null, Integer.valueOf(name), fieldType));
        } else {
            typedParameter.addJPAParameter(new JPAParameter(name, null, fieldType));
        }
    }

    /**
     * Gets the attribute name.
     * 
     * @param fieldName
     *            the field name
     * @return the attribute name
     */
    private String getAttributeName(String fieldName) {
        String attributeName = fieldName;
        if (fieldName.indexOf(".") != -1) {
            attributeName = fieldName.substring(0, fieldName.indexOf("."));
        }
        return attributeName;
    }

    /**
     * Sets the parameter.
     * 
     * @param name
     *            the name
     * @param value
     *            the value
     */

    public final void setParameter(String name, Object value) {
        if (isNative()) {
            bindParameters.add(new BindParameter(name, value));
        } else {
            setParameterValue(":" + name, value);
        }

        parametersMap.put(":" + name, value);
    }

    /**
     * Sets the parameter.
     * 
     * @param position
     *            the position
     * @param value
     *            the value
     */
    public final void setParameter(int position, Object value) {
        if (isNative()) {
            bindParameters.add(new BindParameter(position, value));
        } else {
            setParameterValue("?" + position, value);
        }

        parametersMap.put("?" + position, value);
    }

    /**
     * Sets parameter value into filterClause, depending upon {@link Type}.
     * 
     * @param name
     *            parameter name.
     * @param value
     *            parameter value.
     */
    private void setParameterValue(String name, Object value) {
        if (typedParameter != null) {
            List<FilterClause> clauses =
                typedParameter.getParameters() != null ? typedParameter.getParameters().get(name) : null;
            if (clauses != null) {
                for (FilterClause clause : clauses) {
                    clause.setValue(value);
                }
            } else {
                if (typedParameter.getUpdateParameters() != null) {
                    UpdateClause updateClause = typedParameter.getUpdateParameters().get(name);
                    updateClause.setValue(value);
                } else {
                    logger.error("Error while setting parameter.");
                    throw new QueryHandlerException("named parameter : " + name + " not found!");
                }
            }
        } else {
            throw new QueryHandlerException("No named parameter present for query");
        }
    }

    /**
     * Gets the entity class.
     * 
     * @return the entityClass
     */
    public final Class getEntityClass() {
        return entityClass;
    }

    /**
     * Gets the entity alias.
     * 
     * @return the entity alias
     */
    public final String getEntityAlias() {
        return this.entityAlias;
    }

    /**
     * Checks if is native.
     * 
     * @return true, if is native
     */
    public boolean isNative() {
        return isNativeQuery;
    }

    /**
     * Gets the entity metadata.
     * 
     * @return the entity metadata
     */
    public final EntityMetadata getEntityMetadata() {
        EntityMetadata metadata = null;
        try {
            metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        } catch (KunderaException e) {
            logger.info("No Entity class provided, Proceeding as Scalar Query");
        }
        if (!this.isNativeQuery && metadata == null) {
            throw new KunderaException("Unable to load entity metadata for : " + entityClass);
        }
        return metadata;
    }

    /**
     * Gets the filter clause queue.
     * 
     * @return the filters
     */
    public final Queue getFilterClauseQueue() {
        return filtersQueue;
    }

    /**
     * The FilterClause class to hold a where clause predicate.
     */
    public final class FilterClause {

        /** The property. */
        private String property;

        /** The condition. */
        private String condition;

        /** The condition. */
        private String fieldName;

        /**
         * Gets the field name.
         * 
         * @return the fieldName
         */
        public String getFieldName() {
            return fieldName;
        }

        /** The value. */
        private List<Object> value = new ArrayList<Object>();

        /** Whether to ignore case while evaluating the condition */
        private boolean ignoreCase;

        /**
         * The Constructor.
         * 
         * @param property
         *            the property
         * @param condition
         *            the condition
         * @param value
         *            the value
         * @param fieldName
         *            the field name
         */
        public FilterClause(String property, String condition, Object value, String fieldName) {
            super();
            this.property = property;
            this.condition = condition.trim();
            this.fieldName = fieldName;
            if (value instanceof Collection) {
                for (Object valueObject : (Collection) value) {
                    this.value.add(KunderaQuery.getValue(valueObject));
                }
            } else {
                this.value.add(KunderaQuery.getValue(value));
            }
        }

        /**
         * Gets the property.
         * 
         * @return the property
         */
        public final String getProperty() {
            return property;
        }

        /**
         * Gets the condition.
         * 
         * @return the condition
         */
        public final String getCondition() {
            return condition;
        }

        /**
         * Gets the value.
         * 
         * @return the value
         */
        public final List<Object> getValue() {
            return value;
        }

        /**
         * Sets the value.
         * 
         * @param value
         *            the value to set
         */
        protected void setValue(Object value) {
            List<Object> valObjects = new ArrayList<Object>();
            if (value instanceof Collection) {
                for (Object valueObject : (Collection) value) {
                    valObjects.add(KunderaQuery.getValue(valueObject));
                }
            } else {
                valObjects.add(KunderaQuery.getValue(value));
            }

            this.value = valObjects;
        }

        /**
         * Returns whether to ignore the case when evaluating the filter clause.
         *
         * @return whether to ignore the case when evaluating the filter clause
         */
        public boolean isIgnoreCase() {
            return ignoreCase;
        }

        /**
         * Sets whether to ignore the case when evaluating the filter clause.
         *
         * @param ignoreCase
         *            true to ignore the case when evaluating the filter clause
         */
        public void setIgnoreCase(final boolean ignoreCase) {
            this.ignoreCase = ignoreCase;
        }

        /* @see java.lang.Object#toString() */
        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("FilterClause [property=");
            builder.append(property);
            builder.append(", condition=");
            builder.append(condition);
            builder.append(", value=");
            builder.append(value);
            builder.append(", fieldName=");
            builder.append(fieldName);
            builder.append("]");
            return builder.toString();
        }
    }

    /**
     * The Class UpdateClause.
     */
    public final class UpdateClause {

        /** The property. */
        private String property;

        /** The value. */
        private Object value;

        /**
         * Instantiates a new update clause.
         * 
         * @param property
         *            the property
         * @param value
         *            the value
         */
        public UpdateClause(final String property, final Object value) {
            this.property = property;
            this.value = KunderaQuery.getValue(value);
        }

        /**
         * Gets the property.
         * 
         * @return the property
         */
        public String getProperty() {
            return property;
        }

        /**
         * Gets the value.
         * 
         * @return the value
         */
        public Object getValue() {
            return value;
        }

        /**
         * Sets the value.
         * 
         * @param value
         *            the value to set
         */
        public void setValue(Object value) {
            this.value = KunderaQuery.getValue(value);
        }

    }

    /* @see java.lang.Object#clone() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public final Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /* @see java.lang.Object#toString() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("KunderaQuery [entityName=");
        builder.append(entityName);
        builder.append(", entityAlias=");
        builder.append(entityAlias);
        builder.append(", filtersQueue=");
        builder.append(filtersQueue);
        builder.append("]");
        return builder.toString();
    }

    /**
     * Gets the metamodel.
     * 
     * @param pu
     *            the pu
     * @return the metamodel
     */
    private MetamodelImpl getMetamodel(String pu) {
        return KunderaMetadataManager.getMetamodel(kunderaMetadata, pu);
    }

    /**
     * Gets the persistence units.
     * 
     * @return the persistenceUnits
     */
    public String getPersistenceUnit() {
        return persistenceUnit;
    }

    /**
     * Sets the persistence unit.
     * 
     * @param persistenceUnit
     *            the new persistence unit
     */
    public void setPersistenceUnit(String persistenceUnit) {
        this.persistenceUnit = persistenceUnit;
    }

    /**
     * Parses the ordering @See Order By Clause.
     * 
     * @param ordering
     *            the ordering
     */
    private void parseOrdering(String ordering) {
        final String comma = ",";
        final String space = " ";

        StringTokenizer tokenizer = new StringTokenizer(ordering, comma);

        sortOrders = new ArrayList<KunderaQuery.SortOrdering>();
        while (tokenizer.hasMoreTokens()) {
            String order = (String) tokenizer.nextElement();
            StringTokenizer token = new StringTokenizer(order, space);
            SortOrder orderType = SortOrder.ASC;

            String colName = (String) token.nextElement();
            while (token.hasMoreElements()) {
                String nextOrder = (String) token.nextElement();

                // more spaces given.
                if (StringUtils.isNotBlank(nextOrder)) {
                    try {
                        orderType = SortOrder.valueOf(nextOrder.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        logger.error("Error while parsing order by clause:");
                        throw new JPQLParseException("Invalid sort order provided:" + nextOrder);
                    }
                }
            }
            sortOrders.add(new SortOrdering(colName, orderType));
        }
    }

    /**
     * Containing SortOrder.
     */
    public class SortOrdering {

        /** The column name. */
        String columnName;

        /** The order. */
        SortOrder order;

        /**
         * Instantiates a new sort ordering.
         * 
         * @param columnName
         *            the column name
         * @param order
         *            the order
         */
        public SortOrdering(String columnName, SortOrder order) {
            this.columnName = columnName;
            this.order = order;
        }

        /**
         * Gets the column name.
         * 
         * @return the column name
         */
        public String getColumnName() {
            return columnName;
        }

        /**
         * Gets the order.
         * 
         * @return the order
         */
        public SortOrder getOrder() {
            return order;
        }
    }

    /**
     * The Enum SortOrder.
     */
    public enum SortOrder {
                           /** The ASC. */
                           ASC,
                           /** The DESC. */
                           DESC;
    }

    /**
     * Gets the update clause queue.
     * 
     * @return the updateClauseQueue
     */
    public Queue<UpdateClause> getUpdateClauseQueue() {
        return updateClauseQueue;
    }

    /**
     * Checks if is update clause.
     * 
     * @return true, if is update clause
     */
    public boolean isUpdateClause() {
        return !updateClauseQueue.isEmpty();
    }

    /**
     * Adds the update clause.
     * 
     * @param property
     *            the property
     * @param value
     *            the value
     */
    public void addUpdateClause(final String property, final String value) {
        UpdateClause updateClause = new UpdateClause(property.trim(), value.trim());
        updateClauseQueue.add(updateClause);
        addTypedParameter(
            value.trim().startsWith("?") ? Type.INDEXED : value.trim().startsWith(":") ? Type.NAMED : null, property,
            updateClause);
    }

    /**
     * Adds the filter clause.
     * 
     * @param property
     *            the property
     * @param condition
     *            the condition
     * @param value
     *            the value
     * @param fieldName
     *            the field name
     * @param ignoreCase
     *            to ignore case in the filter
     */
    public void addFilterClause(final String property, final String condition, final Object value,
        final String fieldName, final boolean ignoreCase) {
        if (property != null && condition != null) {
            FilterClause filterClause = new FilterClause(property.trim(), condition.trim(), value, fieldName);
            filterClause.setIgnoreCase(ignoreCase);
            filtersQueue.add(filterClause);
        } else {
            filtersQueue.add(property);
        }
    }

    /**
     * Adds the filter clause.
     * 
     * @param filterClause
     *            the filter clause
     */
    public void addFilterClause(Object filterClause) {

        filtersQueue.add(filterClause);

    }

    /**
     * Sets the checks if is delete update.
     * 
     * @param b
     *            the new checks if is delete update
     */
    public void setIsDeleteUpdate(boolean b) {
        this.isDeleteUpdate = b;
    }

    /**
     * Checks if is delete update.
     * 
     * @return true, if is delete update
     */
    public boolean isDeleteUpdate() {
        return isDeleteUpdate;
    }

    /**
     * Gets the JPA query.
     * 
     * @return the JPA query
     */
    public String getJPAQuery() {
        return this.jpaQuery;
    }

    /**
     * The Class TypedParameter.
     */
    private class TypedParameter {

        /** The type. */
        private Type type;

        /** The jpa parameters. */
        private Set<Parameter<?>> jpaParameters = new HashSet<Parameter<?>>();

        /** The parameters. */
        private Map<String, List<FilterClause>> parameters;

        /** The update parameters. */
        private Map<String, UpdateClause> updateParameters;

        /**
         * Instantiates a new typed parameter.
         * 
         * @param type
         *            the type
         */
        public TypedParameter(Type type) {
            this.type = type;
        }

        /**
         * Gets the type.
         * 
         * @return the type
         */
        private Type getType() {
            return type;
        }

        /**
         * Gets the parameters.
         * 
         * @return the parameters
         */
        Map<String, List<FilterClause>> getParameters() {
            return parameters;
        }

        /**
         * Gets the update parameters.
         * 
         * @return the parameters
         */
        Map<String, UpdateClause> getUpdateParameters() {
            return updateParameters;
        }

        /**
         * Adds the parameters.
         * 
         * @param key
         *            the key
         * @param clause
         *            the clause
         */
        void addParameters(String key, FilterClause clause) {
            if (parameters == null) {
                parameters = new HashMap<String, List<FilterClause>>();
            }
            if (!parameters.containsKey(key)) {
                parameters.put(key, new ArrayList<KunderaQuery.FilterClause>());
            }
            parameters.get(key).add(clause);
        }

        /**
         * Adds the parameters.
         * 
         * @param key
         *            the key
         * @param clause
         *            the clause
         */
        void addParameters(String key, UpdateClause clause) {
            if (updateParameters == null) {
                updateParameters = new HashMap<String, UpdateClause>();
            }

            updateParameters.put(key, clause);
        }

        /**
         * Adds the jpa parameter.
         * 
         * @param param
         *            the param
         */
        void addJPAParameter(Parameter param) {
            jpaParameters.add(param);
        }
    }

    /**
     * The Enum Type.
     */
    private enum Type {

                       /** The indexed. */
                       INDEXED,
                       /** The named. */
                       NAMED
    }

    /*
     * JPA Parameter type
     */
    /**
     * The Class JPAParameter.
     * 
     * @param <T>
     *            the generic type
     */
    private class JPAParameter<T> implements Parameter<T> {

        /** The name. */
        private String name;

        /** The position. */
        private Integer position;

        /** The type. */
        private Class<T> type;

        /**
         * Instantiates a new JPA parameter.
         * 
         * @param name
         *            the name
         * @param position
         *            the position
         * @param type
         *            the type
         */
        JPAParameter(String name, Integer position, Class<T> type) {
            this.name = name;
            this.position = position;
            this.type = type;
        }

        /*
         * (non-Javadoc)
         * 
         * @see javax.persistence.Parameter#getName()
         */
        @Override
        public String getName() {
            return name;
        }

        /*
         * (non-Javadoc)
         * 
         * @see javax.persistence.Parameter#getPosition()
         */
        @Override
        public Integer getPosition() {
            return position;
        }

        /*
         * (non-Javadoc)
         * 
         * @see javax.persistence.Parameter#getParameterType()
         */
        @Override
        public Class<T> getParameterType() {
            return type;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (!obj.getClass().equals(this.getClass())) {
                return false;
            }

            Parameter<?> typed = (Parameter<?>) obj;

            if (typed.getParameterType().equals(this.getParameterType())) {
                if (this.getName() == null && typed.getName() == null) {
                    return this.getPosition() != null && this.getPosition().equals(typed.getPosition());
                } else {
                    return this.getName() != null && this.getName().equals(typed.getName());
                }

            }

            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append("[ name = " + this.getName() + "]");
            strBuilder.append("[ position = " + this.getPosition() + "]");
            strBuilder.append("[ type = " + this.getParameterType() + "]");
            return strBuilder.toString();
        }
    }

    /**
     * Method to skip string literal as per JPA specification. if literal starts is enclose within "''" then skip "'"
     * and include "'" in case of "''" replace it with "'".
     * 
     * @param value
     *            value.
     * 
     * @return replaced string in case of string, else will return original value.
     */
    private static Object getValue(Object value) {
        if (value != null && value.getClass().isAssignableFrom(String.class)) {
            return ((String) value).replaceAll("^'", "").replaceAll("'$", "").replaceAll("''", "'");
        }

        return value;
    }

    /*
     * XXX Indexed or named bind parameter for queries
     */

    public static class BindParameter implements java.io.Serializable {

        private final String name;
        private final int index;
        private final Object value;

        public BindParameter(final String name, final Object value) {
            this.name = name;
            this.index = -1;
            this.value = value;
        }

        public BindParameter(final int index, final Object value) {
            this.name = null;
            this.index = index;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public boolean isNamed() {
            return (getName() != null);
        }

        public int getIndex() {
            return index;
        }

        public Object getValue() {
            return value;
        }
    }

}
