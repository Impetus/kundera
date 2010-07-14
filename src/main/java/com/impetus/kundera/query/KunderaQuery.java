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

public abstract class KunderaQuery {

	public static final String[] SINGLE_STRING_KEYWORDS = { "SELECT", "UPDATE", "DELETE", "UNIQUE", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY" };
	public static final String[] INTER_CLAUSE_OPERATORS = { "AND", "OR" };
	public static final String[] INTRA_CLAUSE_OPERATORS = { "=", "LIKE" };

	private static Pattern INTER_CLAUSE_PATTERN = Pattern.compile("\\band\\b|\\bor\\b", Pattern.CASE_INSENSITIVE);
	private static Pattern INTRA_CLAUSE_PATTERN = Pattern.compile("=|\\blike\\b", Pattern.CASE_INSENSITIVE);

	EntityManagerImpl em;

	private String result;
	private String from;
	private String filter;
	private String ordering;

	private String entityName;
	private String entityAlias;
	private Class<?> entityClass;
	
	// contains a Queue of alternate FilterClause object and Logical Strings (AND, OR etc.)
	public Queue filtersQueue = new LinkedList();

	public KunderaQuery(EntityManagerImpl em) {
		this.em = em;
	}

	public void setGrouping(String groupingClause) {

	}

	public void setResult(String result) {
		this.result = result;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public void setOrdering(String ordering) {
		this.ordering = ordering;
	}

	public String getFilter() {
		return filter;
	}

	public String getFrom() {
		return from;
	}

	public String getOrdering() {
		return ordering;
	}

	public String getResult() {
		return result;
	}

	// must be executed after parse(). it verifies and populated the query predicates.
	protected void postParsingInit() {
		initEntityClass();
		initFilter();
	}

	private void initEntityClass() {
		String result = getResult();
		String from = getFrom();

		String fromArray[] = from.split(" ");
		if (fromArray.length != 2) throw new PersistenceException("Bad query format: " + from);
		if (!fromArray[1].equals(result)) throw new PersistenceException("Bad query format: " + from);

		this.entityName = fromArray[0];
		this.entityAlias = fromArray[1];

		entityClass = em.getMetadataManager().getEntityClassByName(entityName);
		if (null == entityClass) throw new PersistenceException("No entity found by the name: " + entityName);

		EntityMetadata metadata = em.getMetadataManager().getEntityMetadata(entityClass);
		if (!metadata.isIndexable()) throw new PersistenceException(entityClass + " is not indexed. What are you searching for dude?");
	}

	private void initFilter() {
		EntityMetadata metadata = em.getMetadataManager().getEntityMetadata(entityClass);
		String indexName = metadata.getIndexName();

		String filter = getFilter();
		
		if (null == filter) return;
		
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

	public void setParameter(String name, String value) {
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
		if (!found) throw new PersistenceException("invalid parameter: " + name);
	}

	/**
	 * @return the entityClass
	 */
	public Class<?> getEntityClass() {
		return entityClass;
	}

	/**
	 * @return the filters
	 */
	public Queue getFilterClauseQueue() {
		return filtersQueue;
	}

	// class to keep hold of a where clause predicate
	public class FilterClause {
		String property;
		String condition;
		String value;

		/**
		 * @param property
		 * @param condition
		 * @param value
		 */
		public FilterClause(String property, String condition, String value) {
			super();
			this.property = property;
			this.condition = condition;
			this.value = value;
		}

		/**
		 * @return the property
		 */
		public String getProperty() {
			return property;
		}

		/**
		 * @return the condition
		 */
		public String getCondition() {
			return condition;
		}

		/**
		 * @return the value
		 */
		public String getValue() {
			return value;
		}

		/**
		 * @param value
		 *            the value to set
		 */
		protected void setValue(String value) {
			this.value = value;
		}

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

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public String toString() {
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
	private static List<String> tokenize(String where, Pattern pattern) {
		List<String> split = new ArrayList<String>();
		Matcher matcher = pattern.matcher(where);
		int lastIndex = 0;
		String s;
		int i = 0;
		while (matcher.find()) {
			s = where.substring(lastIndex, matcher.start()).trim();
			split.add(s);
			s = matcher.group();
			split.add(s);
			lastIndex = matcher.end();
			i++;
		}
		s = where.substring(lastIndex).trim();
		split.add(s);
		return split;
	}

}
