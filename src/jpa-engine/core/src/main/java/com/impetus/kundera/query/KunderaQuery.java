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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.Parameter;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class KunderaQuery.
 */
public class KunderaQuery
{
    /** The Constant SINGLE_STRING_KEYWORDS. */
    public static final String[] SINGLE_STRING_KEYWORDS = { "SELECT", "UPDATE", "SET", "DELETE", "UNIQUE", "FROM",
            "WHERE", "GROUP BY", "HAVING", "ORDER BY" };

    /** The Constant INTER_CLAUSE_OPERATORS. */
    public static final String[] INTER_CLAUSE_OPERATORS = { "AND", "OR", "BETWEEN", "(", ")" };

    /** The Constant INTRA_CLAUSE_OPERATORS. */
    public static final String[] INTRA_CLAUSE_OPERATORS = { "=", "LIKE", "IN", ">", ">=", "<", "<=", "<>", "NOT IN" };

    /** The INTER pattern. */
    private static final Pattern INTER_CLAUSE_PATTERN = Pattern.compile(
            "\\s\\band\\b\\s|\\s\\bor\\b\\s|\\s\\bbetween\\b\\s|\\(|\\)", Pattern.CASE_INSENSITIVE);

    /** The INTRA pattern. */
    private static final Pattern INTRA_CLAUSE_PATTERN = Pattern.compile("=|\\s\\blike\\b|\\bin\\b|\\bnot in\\b|>=|>|<=|<|<>|\\s\\bset",
            Pattern.CASE_INSENSITIVE);

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KunderaQuery.class);

    /** The result. */
    private String[] result;

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

    /** Persistence Unit(s). */
    private String persistenceUnit;

    // contains a Queue of alternate FilterClause object and Logical Strings
    // (AND, OR etc.)
    /** The filters queue. */
    private Queue filtersQueue = new LinkedList();

    private boolean isDeleteUpdate;

    private Queue<UpdateClause> updateClauseQueue = new LinkedList<UpdateClause>();

    private TypedParameter typedParameter;

    boolean isNativeQuery;

    private String jpaQuery;

    private final KunderaMetadata kunderaMetadata;

    /**
     * Instantiates a new kundera query.
     * 
     * @param persistenceUnits
     *            the persistence units
     */
    public KunderaQuery(final String jpaQuery, final KunderaMetadata kunderaMetadata)
    {
        this.jpaQuery = jpaQuery;
        this.kunderaMetadata = kunderaMetadata;
    }

    /**
     * Sets the grouping.
     * 
     * @param groupingClause
     *            the new grouping
     */
    public void setGrouping(String groupingClause)
    {
    }

    /**
     * Sets the result.
     * 
     * @param result
     *            the new result
     */
    public final void setResult(String... result)
    {
        this.result = result;
    }

    /**
     * Sets the from.
     * 
     * @param from
     *            the new from
     */
    public final void setFrom(String from)
    {
        this.from = from;
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    public final void setFilter(String filter)
    {
        this.filter = filter;
    }

    /**
     * Sets the ordering.
     * 
     * @param ordering
     *            the new ordering
     */
    public final void setOrdering(String ordering)
    {
        this.ordering = ordering;
        parseOrdering(this.ordering);
    }

    /**
     * Gets the filter.
     * 
     * @return the filter
     */
    public final String getFilter()
    {
        return filter;
    }

    /**
     * Gets the from.
     * 
     * @return the from
     */
    public final String getFrom()
    {
        return from;
    }

    /**
     * Gets the ordering.
     * 
     * @return the ordering
     */
    public final List<SortOrdering> getOrdering()
    {
        return sortOrders;
    }

    /**
     * Gets the result.
     * 
     * @return the result
     */
    public final String[] getResult()
    {
        return result;
    }

    /**
     * Method to check if required result is to get complete entity or a select
     * scalar value.
     * 
     * @return true, if it result is for complete alias.
     * 
     */
    public final boolean isAliasOnly()
    {
        // TODO
        return result != null && (result[0].indexOf(".") == -1);
    }

    /**
     * Returns set of parameters.
     * 
     * @return jpaParameters
     */
    public Set<Parameter<?>> getParameters()
    {
        return typedParameter != null ? typedParameter.jpaParameters : null;
    }

    /**
     * Parameter is bound if it holds any value, else will return false
     * 
     * @param param
     * @return
     */
    public boolean isBound(Parameter param)
    {
        return getClauseValue(param) != null;
    }

    /**
     * Returns clause value for supplied parameter.
     * 
     * @param paramString
     * @return
     */
    public List<Object> getClauseValue(String paramString)
    {
        if (typedParameter != null && typedParameter.getParameters() != null)
        {
            List<FilterClause> clauses = typedParameter.getParameters().get(paramString);
            if (clauses != null)
            {
                return clauses.get(0).getValue();
            }
            else
            {
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
    public List<Object> getClauseValue(Parameter param)
    {
        Parameter match = null;
        if (typedParameter != null && typedParameter.jpaParameters != null)
        {
            for (Parameter p : typedParameter.jpaParameters)
            {
                if (p.equals(param))
                {
                    match = p;
                    if (typedParameter.getType().equals(Type.NAMED))
                    {
                        List<FilterClause> clauses = typedParameter.getParameters().get(":" + p.getName());
                        if (clauses != null)
                        {
                            return clauses.get(0).getValue();
                        }
                    }
                    else
                    {
                    	List<FilterClause> clauses = typedParameter.getParameters().get("?" + p.getPosition());
                        if (clauses != null)
                        {
                            return clauses.get(0).getValue();
                        }
                        else
                        {
                            UpdateClause updateClause = typedParameter.getUpdateParameters().get("?" + p.getPosition());
                            if (updateClause != null)
                            {
                                List<Object> value = new ArrayList<Object>();
                                value.add(updateClause.getValue());
                                return value;
                            }

                        }
                    }
                    break;
                }
            }
            if (match == null)
            {
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
    protected void postParsingInit()
    {
        initEntityClass();
        initFilter();
        initUpdateClause();
    }

    /**
     * 
     */
    private void initUpdateClause()
    {
        for (UpdateClause updateClause : updateClauseQueue)
        {
            onTypedParameter(updateClause.getValue(), updateClause, updateClause.getProperty().trim());
        }

    }

    /**
     * Inits the entity class.
     */
    private void initEntityClass()
    {
        if (from == null)
        {
            throw new JPQLParseException("Bad query format FROM clause is mandatory for SELECT queries");
        }
        String fromArray[] = from.split(" ");

        if (!this.isDeleteUpdate)
        {
            if (fromArray.length != 2)
            {
                throw new JPQLParseException("Bad query format: " + from
                        + ". Identification variable is mandatory in FROM clause for SELECT queries");
            }

            // TODO
            StringTokenizer tokenizer = new StringTokenizer(getResult()[0], ",");
            while (tokenizer.hasMoreTokens())
            {
                String token = tokenizer.nextToken();
                if (!StringUtils.containsAny(fromArray[1] + ".", token))
                {
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

        if (model != null)
        {
            entityClass = model.getEntityClass(entityName);
        }

        if (null == entityClass)
        {
            logger.error(
                    "No entity {} found, please verify it is properly annotated with @Entity and not a mapped Super class",
                    entityName);
            throw new QueryHandlerException("No entity found by the name: " + entityName);
        }

        EntityMetadata metadata = model.getEntityMetadata(entityClass);

        if (metadata != null && !metadata.isIndexable())
        {
            throw new QueryHandlerException(entityClass + " is not indexed. Not possible to run a query on it."
                    + " Check whether it was properly annotated for indexing.");
        }
    }

    /**
     * Inits the filter.
     */
    private void initFilter()
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        // String indexName = metadata.getIndexName();

        // String filter = getFilter();

        Metamodel metaModel = kunderaMetadata.getApplicationMetadata().getMetamodel(getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityClass);

        if (null == filter)
        {
            List<String> clauses = new ArrayList<String>();
            addDiscriminatorClause(clauses, entityType);
            return;
        }

        List<String> clauses = tokenize(filter, INTER_CLAUSE_PATTERN);

        // parse and structure for "between" clause , if present, else it will
        // return original clause
        clauses = parseFilterForBetweenClause(clauses);
        // clauses must be alternate Inter and Intra combination, starting with
        // Intra.
        boolean newClause = true;

        for (String clause : clauses)
        {
        	if(clause.trim().equals("(") || clause.trim().equals(")"))
        	{
                filtersQueue.add(clause.trim());
                newClause = true;
            }
        	else if (newClause)
            {
                List<String> tokens = tokenize(clause, INTRA_CLAUSE_PATTERN);

                if (tokens.size() != 3)
                {
                    throw new PersistenceException("bad jpa query: " + clause);
                }

                // strip alias from property name
                String property = tokens.get(0);
                if (property.indexOf(".") > 0)
                {
                    property = property.substring((entityAlias + ".").length());
                }

                String columnName = null;
                try
                {
                    columnName = ((AbstractAttribute) entityType.getAttribute(property)).getJPAColumnName();
                }
                catch (IllegalArgumentException iaex)
                {
                    logger.warn("No column found by this name : " + property + " checking for embeddedfield");
                }
                // where condition may be for search within embedded object
                if (columnName == null && property.indexOf(".") > 0)
                {
                    String enclosingEmbeddedField = MetadataUtils.getEnclosingEmbeddedFieldName(metadata, property,
                            true, kunderaMetadata);
                    if (enclosingEmbeddedField != null)
                    {
                        columnName = property;
                    }
                }

                if (columnName == null)
                {
                    logger.error("No column found by this name : " + property);
                    throw new JPQLParseException("No column found by this name : " + property + ". Check your query.");
                }

                String condition = tokens.get(1);
                if (!Arrays.asList(INTRA_CLAUSE_OPERATORS).contains(condition.toUpperCase().trim()))
                {
                    throw new JPQLParseException("Bad JPA query: " + clause);
                }

                FilterClause filterClause = new FilterClause(

                columnName, condition, tokens.get(2));
                filtersQueue.add(filterClause);

                onTypedParameter(tokens, filterClause, property);
                newClause = false;
            }
            else
            {
                if (Arrays.asList(INTER_CLAUSE_OPERATORS).contains(clause.toUpperCase().trim()))
                {
                    filtersQueue.add(clause.toUpperCase().trim());
                    newClause = true;
                }
                else
                {
                    throw new JPQLParseException("bad jpa query: " + clause);
                }
            }
        }

        addDiscriminatorClause(clauses, entityType);
    }

    private void addDiscriminatorClause(List<String> clauses, EntityType entityType)
    {
        if (((AbstractManagedType) entityType).isInherited())
        {
            String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
            String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

            if (discrColumn != null && discrValue != null)
            {
                if (!clauses.isEmpty())
                {
                    filtersQueue.add("AND");
                }

                FilterClause filterClause = new FilterClause(discrColumn, "=", discrValue);
                filtersQueue.add(filterClause);
            }
        }
    }

    /**
     * Depending upon filter value, if it starts with ":" then it is NAMED
     * parameter, else if starts with "?", it will be INDEXED parameter.
     * 
     * @param tokens
     *            tokens
     * @param filterClause
     *            filter clauses.
     */
    private void onTypedParameter(Object value, UpdateClause updateClause, String fieldName)
    {
        String token = value.toString();
        if (token != null && token.startsWith(":"))
        {
            addTypedParameter(Type.NAMED, token, updateClause);
            filterJPAParameterInfo(Type.NAMED, token.substring(1), fieldName);
        }
        else if (token != null && token.startsWith("?"))
        {
            addTypedParameter(Type.INDEXED, token, updateClause);
            filterJPAParameterInfo(Type.INDEXED, token.substring(1), fieldName);
        }
    }

    /**
     * Depending upon filter value, if it starts with ":" then it is NAMED
     * parameter, else if starts with "?", it will be INDEXED parameter.
     * 
     * @param tokens
     *            tokens
     * @param filterClause
     *            filter clauses.
     */
    private void onTypedParameter(List<String> tokens, FilterClause filterClause, String fieldName)
    {
        if (tokens.get(2) != null && tokens.get(2).startsWith(":"))
        {
            addTypedParameter(Type.NAMED, tokens.get(2), filterClause);
            filterJPAParameterInfo(Type.NAMED, tokens.get(2).substring(1), fieldName);
        }
        else if (tokens.get(2) != null && tokens.get(2).startsWith("?"))
        {
            addTypedParameter(Type.INDEXED, tokens.get(2), filterClause);
            filterJPAParameterInfo(Type.INDEXED, tokens.get(2).substring(1), fieldName);
        }
    }

    /**
     * Adds typed parameter to {@link TypedParameter}
     * 
     * @param type
     *            type of parameter(e.g. NAMED/INDEXED)
     * @param parameter
     *            parameter name.
     * @param clause
     *            filter clause.
     */
    private void addTypedParameter(Type type, String parameter, FilterClause clause)
    {
        if (typedParameter == null)
        {
            typedParameter = new TypedParameter(type);
        }

        if (typedParameter.getType().equals(type))
        {
            typedParameter.addParameters(parameter, clause);
        }
        else
        {
            logger.warn("Invalid type provided, it can either be name or indexes!");
        }
    }

    /**
     * Adds typed parameter to {@link TypedParameter}
     * 
     * @param type
     *            type of parameter(e.g. NAMED/INDEXED)
     * @param parameter
     *            parameter name.
     * @param clause
     *            filter clause.
     */
    private void addTypedParameter(Type type, String parameter, UpdateClause clause)
    {
        if (type != null)
        {
            if (typedParameter == null)
            {
                typedParameter = new TypedParameter(type);
            }

            if (typedParameter.getType().equals(type))
            {
                typedParameter.addParameters(parameter, clause);
            }
            else
            {
                logger.warn("Invalid type provided, it can either be name or indexes!");
            }
        }
    }

    private void filterJPAParameterInfo(Type type, String name, String fieldName)
    {
        String attributeName = getAttributeName(fieldName);

        Attribute entityAttribute = ((MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                persistenceUnit)).getEntityAttribute(entityClass, attributeName);
        Class fieldType = entityAttribute.getJavaType();

        if (type.equals(Type.INDEXED))
        {
            typedParameter.addJPAParameter(new JPAParameter(null, Integer.valueOf(name), fieldType));
        }
        else
        {
            typedParameter.addJPAParameter(new JPAParameter(name, null, fieldType));
        }
    }

    private String getAttributeName(String fieldName)
    {
        String attributeName = fieldName;
        if (fieldName.indexOf(".") != -1)
        {
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
    public final void setParameter(String name, Object value)
    {
        setParameterValue(":" + name, value);
    }

    public final void setParameter(int position, Object value)
    {
        setParameterValue("?" + position, value);
    }

    /**
     * Sets parameter value into filterClause, depending upon {@link Type}
     * 
     * @param name
     *            parameter name.
     * @param value
     *            parameter value.
     */
    private void setParameterValue(String name, Object value)
    {
        if (typedParameter != null)
        {
            List<FilterClause> clauses = typedParameter.getParameters() != null ? typedParameter.getParameters().get(name)
                    : null;
            if (clauses != null)
            {
            	for (FilterClause clause : clauses) {
            		clause.setValue(value);
				}
            }
            else
            {
                if (typedParameter.getUpdateParameters() != null)
                {
                    UpdateClause updateClause = typedParameter.getUpdateParameters().get(name);
                    updateClause.setValue(value);
                }
                else
                {
                    logger.error("Error while setting parameter.");
                    throw new QueryHandlerException("named parameter : " + name + " not found!");
                }
            }
        }
        else
        {
            throw new QueryHandlerException("No named parameter present for query");
        }
    }

    /**
     * Gets the entity class.
     * 
     * @return the entityClass
     */
    public final Class getEntityClass()
    {
        return entityClass;
    }

    public boolean isNative()
    {
        return isNativeQuery;
    }

    /**
     * Gets the entity metadata.
     * 
     * @return the entity metadata
     */
    public final EntityMetadata getEntityMetadata()
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        if (metadata == null)
        {
            throw new KunderaException("Unable to load entity metadata for : " + entityClass);
        }
        return metadata;
    }

    /**
     * Gets the filter clause queue.
     * 
     * @return the filters
     */
    public final Queue getFilterClauseQueue()
    {
        return filtersQueue;
    }

    /**
     * The FilterClause class to hold a where clause predicate.
     */
    public final class FilterClause
    {

        /** The property. */
        private String property;

        /** The condition. */
        private String condition;

        /** The value. */
        private List<Object> value = new ArrayList<Object>();

        /**
         * The Constructor.
         * 
         * @param property
         *            the property
         * @param condition
         *            the condition
         * @param value
         *            the value
         */
        public FilterClause(String property, String condition, Object value)
        {
            super();
            this.property = property;
            this.condition = condition.trim();
            if (value instanceof Collection)
            {
                for (Object valueObject : (Collection) value)
                {
                    this.value.add(KunderaQuery.getValue(valueObject));
                }
            }
            else
            {
                this.value.add(KunderaQuery.getValue(value));
            }
        }

        /**
         * Gets the property.
         * 
         * @return the property
         */
        public final String getProperty()
        {
            return property;
        }

        /**
         * Gets the condition.
         * 
         * @return the condition
         */
        public final String getCondition()
        {
            return condition;
        }

        /**
         * Gets the value.
         * 
         * @return the value
         */
        public final List<Object> getValue()
        {
            return value;
        }

        /**
         * Sets the value.
         * 
         * @param value
         *            the value to set
         */
        protected void setValue(Object value)
        {
            List<Object> valObjects = new ArrayList<Object>();
            if (value instanceof Collection)
            {
                for (Object valueObject : (Collection) value)
                {
                    valObjects.add(KunderaQuery.getValue(valueObject));
                }
            }
            else
            {
                valObjects.add(KunderaQuery.getValue(value));
            }

            this.value = valObjects;
        }

        /* @see java.lang.Object#toString() */
        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("FilterClause [property=");
            builder.append(property);
            builder.append(", condition=");
            builder.append(condition);
            builder.append(", value=");
            builder.append(value);
            builder.append("]");
            return builder.toString();
        }
    }

    public final class UpdateClause
    {
        private String property;

        private Object value;

        public UpdateClause(final String property, final Object value)
        {
            this.property = property;
            this.value = KunderaQuery.getValue(value);
        }

        /**
         * @return the property
         */
        public String getProperty()
        {
            return property;
        }

        /**
         * @return the value
         */
        public Object getValue()
        {
            return value;
        }

        /**
         * @param value
         *            the value to set
         */
        public void setValue(Object value)
        {
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
    public final Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }

    /* @see java.lang.Object#toString() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString()
    {
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

    // helper method
    /**
     * Tokenize.
     * 
     * @param where
     *            the where
     * @param pattern
     *            the pattern
     * @return the list
     */
    private static List<String> tokenize(String where, Pattern pattern)
    {
        List<String> split = new ArrayList<String>();
        Matcher matcher = pattern.matcher(where);
        int lastIndex = 0;
        String s;
        // int count = 0;
        while (matcher.find())
        {
            s = where.substring(lastIndex, matcher.start()).trim();
            if(!s.equals(""))
            	split.add(s);
            s = matcher.group();
            split.add(s.toUpperCase());
            lastIndex = matcher.end();
            // count++;
        }
        s = where.substring(lastIndex).trim();
        if(!s.equals(""))
        	split.add(s);
        return split;
    }

    /**
     * Gets the metamodel.
     * 
     * @return the metamodel
     */
    private MetamodelImpl getMetamodel(String pu)
    {
        return KunderaMetadataManager.getMetamodel(kunderaMetadata, pu);
    }

    /**
     * Gets the persistence units.
     * 
     * @return the persistenceUnits
     */
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    /**
     * Parses the ordering @See Order By Clause.
     * 
     * @param ordering
     *            the ordering
     */
    private void parseOrdering(String ordering)
    {
        final String comma = ",";
        final String space = " ";

        StringTokenizer tokenizer = new StringTokenizer(ordering, comma);

        sortOrders = new ArrayList<KunderaQuery.SortOrdering>();
        while (tokenizer.hasMoreTokens())
        {
            String order = (String) tokenizer.nextElement();
            StringTokenizer token = new StringTokenizer(order, space);
            SortOrder orderType = SortOrder.ASC;

            String colName = (String) token.nextElement();
            while (token.hasMoreElements())
            {
                String nextOrder = (String) token.nextElement();

                // more spaces given.
                if (StringUtils.isNotBlank(nextOrder))
                {
                    try
                    {
                        orderType = SortOrder.valueOf(nextOrder);
                    }
                    catch (IllegalArgumentException e)
                    {
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
    public class SortOrdering
    {

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
        public SortOrdering(String columnName, SortOrder order)
        {
            this.columnName = columnName;
            this.order = order;
        }

        /**
         * Gets the column name.
         * 
         * @return the column name
         */
        public String getColumnName()
        {
            return columnName;
        }

        /**
         * Gets the order.
         * 
         * @return the order
         */
        public SortOrder getOrder()
        {
            return order;
        }
    }

    /**
     * The Enum SortOrder.
     */
    public enum SortOrder
    {
        /** The ASC. */
        ASC,
        /** The DESC. */
        DESC;
    }

    /**
     * @return the updateClauseQueue
     */
    public Queue<UpdateClause> getUpdateClauseQueue()
    {
        return updateClauseQueue;
    }

    public boolean isUpdateClause()
    {
        return !updateClauseQueue.isEmpty();
    }

    public void addUpdateClause(final String property, final String value)
    {
        UpdateClause updateClause = new UpdateClause(property.trim(), value.trim());
        updateClauseQueue.add(updateClause);
        addTypedParameter(value.trim().startsWith("?") ? Type.INDEXED : value.trim().startsWith(":") ? Type.NAMED
                : null, property, updateClause);
    }

    /**
     * @param b
     */
    public void setIsDeleteUpdate(boolean b)
    {
        this.isDeleteUpdate = b;
    }

    public boolean isDeleteUpdate()
    {
        return isDeleteUpdate;
    }

    public String getJPAQuery()
    {
        return this.jpaQuery;
    }

    /**
     * Return parsed token string.
     * 
     * @param tokens
     *            inter claues token string.
     * @param indexName
     *            table name
     * @return tokens converted to "<=" and ">=" clause
     */
    private List<String> parseFilterForBetweenClause(List<String> tokens)
    {
        // There should be whitespace on bothside of keyword between.
        final String between = " BETWEEN ";

        if (tokens.contains(between))
        {
            // change token set to parse and compile.
            int idxOfBetween = tokens.indexOf(between);
            String property = tokens.get(idxOfBetween - 1);
            Matcher match = INTRA_CLAUSE_PATTERN.matcher(property);
            // in case any intra clause given along with column name.
            if (match.find())
            {
                logger.error("bad jpa query:");
                throw new JPQLParseException("invalid column name" + property);
            }
            String minValue = tokens.get(idxOfBetween + 1);
            String maxValue = tokens.get(idxOfBetween + 3);

            tokens.set(idxOfBetween + 1, property + ">=" + minValue);
            tokens.set(idxOfBetween + 3, property + "<=" + maxValue);
            tokens.remove(idxOfBetween - 1);
            tokens.remove(idxOfBetween - 1);
        }

        return tokens;
    }

    private class TypedParameter
    {
        private Type type;

        private Set<Parameter<?>> jpaParameters = new HashSet<Parameter<?>>();

        private Map<String, List<FilterClause>> parameters;

        private Map<String, UpdateClause> updateParameters;

        /**
         * 
         */
        public TypedParameter(Type type)
        {
            this.type = type;
        }

        /**
         * @return the type
         */
        private Type getType()
        {
            return type;
        }

        /**
         * @return the parameters
         */
        Map<String, List<FilterClause>> getParameters()
        {
            return parameters;
        }

        /**
         * @return the parameters
         */
        Map<String, UpdateClause> getUpdateParameters()
        {
            return updateParameters;
        }

        void addParameters(String key, FilterClause clause)
        {
            if (parameters == null)
            {
                parameters = new HashMap<String, List<FilterClause>>();
            }
            if(!parameters.containsKey(key)) {
            	parameters.put(key, new ArrayList<KunderaQuery.FilterClause>());
            }
            parameters.get(key).add(clause);
        }

        void addParameters(String key, UpdateClause clause)
        {
            if (updateParameters == null)
            {
                updateParameters = new HashMap<String, UpdateClause>();
            }

            updateParameters.put(key, clause);
        }

        void addJPAParameter(Parameter param)
        {
            jpaParameters.add(param);
        }
    }

    private enum Type
    {
        INDEXED, NAMED
    }

    /*
     * JPA Parameter type
     */
    private class JPAParameter<T> implements Parameter<T>
    {
        private String name;

        private Integer position;

        private Class<T> type;

        /**
         * 
         */
        JPAParameter(String name, Integer position, Class<T> type)
        {
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
        public String getName()
        {
            return name;
        }

        /*
         * (non-Javadoc)
         * 
         * @see javax.persistence.Parameter#getPosition()
         */
        @Override
        public Integer getPosition()
        {
            return position;
        }

        /*
         * (non-Javadoc)
         * 
         * @see javax.persistence.Parameter#getParameterType()
         */
        @Override
        public Class<T> getParameterType()
        {
            return type;
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode(this);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (!obj.getClass().equals(this.getClass()))
            {
                return false;
            }

            Parameter<?> typed = (Parameter<?>) obj;

            if (typed.getParameterType().equals(this.getParameterType()))
            {
                if (this.getName() == null && typed.getName() == null)
                {
                    return this.getPosition() != null && this.getPosition().equals(typed.getPosition());
                }
                else
                {
                    return this.getName() != null && this.getName().equals(typed.getName());
                }

            }

            return false;
        }

        @Override
        public String toString()
        {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append("[ name = " + this.getName() + "]");
            strBuilder.append("[ position = " + this.getPosition() + "]");
            strBuilder.append("[ type = " + this.getParameterType() + "]");
            return strBuilder.toString();
        }
    }

    /**
     * Method to skip string literal as per JPA specification. if literal starts
     * is enclose within "''" then skip "'" and include "'" in case of "''"
     * replace it with "'".
     * 
     * @param value
     *            value.
     * 
     * @return replaced string in case of string, else will return original
     *         value.
     */
    private static Object getValue(Object value)
    {
        if (value != null && value.getClass().isAssignableFrom(String.class))
        {
            return ((String) value).replaceAll("^'", "").replaceAll("'$", "").replaceAll("''", "'");
        }

        return value;
    }

}