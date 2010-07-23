/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.PersistenceException;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;

/**
 * The Class KunderaQuery.
 */
public abstract class KunderaQuery {

    /** The Constant SINGLE_STRING_KEYWORDS. */
    public static final String[] SINGLE_STRING_KEYWORDS = { "SELECT", "UPDATE", "DELETE", "UNIQUE", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY" };

    /** The Constant INTER_CLAUSE_OPERATORS. */
    public static final String[] INTER_CLAUSE_OPERATORS = { "AND", "OR" };

    /** The Constant INTRA_CLAUSE_OPERATORS. */
    public static final String[] INTRA_CLAUSE_OPERATORS = { "=", "LIKE" };

    /** The INTER pattern. */
    private static final Pattern INTER_CLAUSE_PATTERN = Pattern.compile("\\band\\b|\\bor\\b", Pattern.CASE_INSENSITIVE);

    /** The INTRA pattern. */
    private static final Pattern INTRA_CLAUSE_PATTERN = Pattern.compile("=|\\blike\\b", Pattern.CASE_INSENSITIVE);

    /** The em. */
    private EntityManagerImpl em;

    /** The result. */
    private String result;

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

    // contains a Queue of alternate FilterClause object and Logical Strings
    // (AND, OR etc.)
    /** The filters queue. */
    private Queue filtersQueue = new LinkedList();

    /**
     * Instantiates a new kundera query.
     * 
     * @param em
     *            the em
     */
    public KunderaQuery(EntityManagerImpl em) {
        this.em = em;
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
    public final void setResult(String result) {
        this.result = result;
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
    public final String getOrdering() {
        return ordering;
    }

    /**
     * Gets the result.
     * 
     * @return the result
     */
    public final String getResult() {
        return result;
    }

    // must be executed after parse(). it verifies and populated the query
    // predicates.
    /**
     * Post parsing init.
     */
    protected void postParsingInit() {
        initEntityClass();
        initFilter();
    }

    /**
     * Inits the entity class.
     */
    private void initEntityClass() {
        // String result = getResult();
        // String from = getFrom();

        String fromArray[] = from.split(" ");
        if (fromArray.length != 2) {
            throw new PersistenceException("Bad query format: " + from);
        }
        if (!fromArray[1].equals(result)) {
            throw new PersistenceException("Bad query format: " + from);
        }
        this.entityName = fromArray[0];
        this.entityAlias = fromArray[1];

        entityClass = em.getMetadataManager().getEntityClassByName(entityName);
        if (null == entityClass) {
            throw new PersistenceException("No entity found by the name: " + entityName);
        }
        EntityMetadata metadata = em.getMetadataManager().getEntityMetadata(entityClass);
        if (!metadata.isIndexable()) {
            throw new PersistenceException(entityClass + " is not indexed. What are you searching for dude?");
        }
    }

    /**
     * Inits the filter.
     */
    private void initFilter() {
        EntityMetadata metadata = em.getMetadataManager().getEntityMetadata(entityClass);
        String indexName = metadata.getIndexName();

        // String filter = getFilter();

        if (null == filter) {
            return;
        }

        List<String> clauses = tokenize(filter, INTER_CLAUSE_PATTERN);
        // clauses must be alternate Inter and Intra conbination, starting with
        // Intra.
        boolean newClause = true;
        for (String clause : clauses) {

            if (newClause) {
                List<String> tokens = tokenize(clause, INTRA_CLAUSE_PATTERN);

                if (tokens.size() != 3) {
                    throw new PersistenceException("bad jpa query: " + clause);
                }

                // strip alias from property name
                String property = tokens.get(0);
                property = property.substring((entityAlias + ".").length());
                property = indexName + "." + property;
                // verify condition
                String condition = tokens.get(1);
                if (!Arrays.asList(INTRA_CLAUSE_OPERATORS).contains(condition.toUpperCase())) {
                    throw new PersistenceException("bad jpa query: " + clause);
                }

                filtersQueue.add(new FilterClause(property, condition, tokens.get(2)));
                newClause = false;
            }

            else {
                if (Arrays.asList(INTER_CLAUSE_OPERATORS).contains(clause.toUpperCase())) {
                    filtersQueue.add(clause.toUpperCase());
                    newClause = true;
                } else {
                    throw new PersistenceException("bad jpa query: " + clause);
                }
            }
        }
    }

    /**
     * Sets the parameter.
     * 
     * @param name
     *            the name
     * @param value
     *            the value
     */
    public final void setParameter(String name, String value) {
        boolean found = false;
        for (Object object : getFilterClauseQueue()) {
            if (object instanceof FilterClause) {
                FilterClause filter = (FilterClause) object;
                // key
                if (filter.getValue().equals(":" + name)) {
                    filter.setValue(value);
                    found = true;
                    return;
                }
            }
        }
        if (!found) {
            throw new PersistenceException("invalid parameter: " + name);
        }
    }

    /**
     * Gets the entity class.
     * 
     * @return the entityClass
     */
    public final Class<?> getEntityClass() {
        return entityClass;
    }

    /**
     * Gets the filter clause queue.
     * 
     * @return the filters
     */
    public final Queue getFilterClauseQueue() {
        return filtersQueue;
    }

    // class to keep hold of a where clause predicate
    /**
     * The Class FilterClause.
     */
    public final class FilterClause {

        /** The property. */
        private String property;

        /** The condition. */
        private String condition;

        /** The value. */
        String value;

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
        public FilterClause(String property, String condition, String value) {
            super();
            this.property = property;
            this.condition = condition;
            this.value = value;
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
        public final String getValue() {
            return value;
        }

        /**
         * Sets the value.
         * 
         * @param value
         *            the value to set
         */
        protected void setValue(String value) {
            this.value = value;
        }

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
            builder.append("]");
            return builder.toString();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public final Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

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

    // helper method
    /**
     * Tokenize.
     * 
     * @param where
     *            the where
     * @param pattern
     *            the pattern
     * 
     * @return the list< string>
     */
    private static List<String> tokenize(String where, Pattern pattern) {
        List<String> split = new ArrayList<String>();
        Matcher matcher = pattern.matcher(where);
        int lastIndex = 0;
        String s;
        // int count = 0;
        while (matcher.find()) {
            s = where.substring(lastIndex, matcher.start()).trim();
            split.add(s);
            s = matcher.group();
            split.add(s);
            lastIndex = matcher.end();
            // count++;
        }
        s = where.substring(lastIndex).trim();
        split.add(s);
        return split;
    }

}
