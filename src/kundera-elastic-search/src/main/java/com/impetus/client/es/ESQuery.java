/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.es;

import java.util.Iterator;
import java.util.List;

import javax.persistence.Query;
import javax.persistence.metamodel.EntityType;

import org.eclipse.persistence.jpa.jpql.parser.AggregateFunction;
import org.eclipse.persistence.jpa.jpql.parser.AndExpression;
import org.eclipse.persistence.jpa.jpql.parser.CollectionExpression;
import org.eclipse.persistence.jpa.jpql.parser.ComparisonExpression;
import org.eclipse.persistence.jpa.jpql.parser.Expression;
import org.eclipse.persistence.jpa.jpql.parser.IdentificationVariable;
import org.eclipse.persistence.jpa.jpql.parser.LogicalExpression;
import org.eclipse.persistence.jpa.jpql.parser.NullExpression;
import org.eclipse.persistence.jpa.jpql.parser.OrExpression;
import org.eclipse.persistence.jpa.jpql.parser.SelectClause;
import org.eclipse.persistence.jpa.jpql.parser.SelectStatement;
import org.eclipse.persistence.jpa.jpql.parser.StateFieldPathExpression;
import org.eclipse.persistence.jpa.jpql.parser.SubExpression;
import org.eclipse.persistence.jpa.jpql.parser.WhereClause;
import org.eclipse.persistence.jpa.jpql.utility.iterable.ListIterable;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * @author vivek.mishra Implementation of query interface {@link Query}.
 */

public class ESQuery<E> extends QueryImpl
{


	public ESQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator, final KunderaMetadata kunderaMetadata)
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
		MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
				m.getPersistenceUnit());
		EntityType entity = metaModel.entity(m.getEntityClazz());
		Expression whereExpression = getKunderaQuery().getSelectStatement().getWhereClause();

		FilterBuilder filter = whereExpression instanceof NullExpression ? null : populateFilterBuilder(((WhereClause)whereExpression).getConditionalExpression(), m, entity); 
		return ((ESClient) client).executeQuery(filter, useAggregation(kunderaQuery, m, filter), m, getKunderaQuery());
	}

	/**
	 * @param condtionalExp
	 * @param m
	 * @param entity
	 * @return
	 */
	public FilterBuilder populateFilterBuilder(Expression condtionalExp, EntityMetadata m, EntityType entity) 
	{	
		FilterBuilder filter = null;
		condtionalExp = (condtionalExp instanceof SubExpression) ? ((SubExpression)condtionalExp).getExpression() : condtionalExp ;
		filter = (condtionalExp instanceof ComparisonExpression) ? getFilter(populateFilterClause((ComparisonExpression)condtionalExp), m, entity) : filter ;
		filter = (condtionalExp instanceof LogicalExpression) ? populateLogicalFilterBuilder(condtionalExp, m , entity) : filter ;
		
		return filter;
	}

	/**
	 * @param logicalExp
	 * @param m
	 * @param entity
	 * @return
	 */
	private FilterBuilder populateLogicalFilterBuilder(Expression logicalExp, EntityMetadata m, EntityType entity)
	{
		String identifier = ((LogicalExpression)logicalExp).getIdentifier(); 

		return (identifier.equalsIgnoreCase(LogicalExpression.AND)) ? getAndFilterBuilder(logicalExp, m, entity) : 
			(identifier.equalsIgnoreCase(LogicalExpression.OR)) ? getOrFilterBuilder(logicalExp, m, entity) : null ;  
	}

	/**
	 * @param logicalExp
	 * @param m
	 * @param entity
	 * @return
	 */
	private AndFilterBuilder getAndFilterBuilder(Expression logicalExp, EntityMetadata m, EntityType entity)
	{
		AndExpression andExp = (AndExpression)logicalExp;
		Expression leftExpression = andExp.getLeftExpression();
		Expression rightExpression = andExp.getRightExpression();

		return new AndFilterBuilder(populateFilterBuilder(leftExpression, m, entity), populateFilterBuilder(rightExpression, m, entity));
	}

	/**
	 * @param logicalExp
	 * @param m
	 * @param entity
	 * @return
	 */
	private OrFilterBuilder getOrFilterBuilder(Expression logicalExp, EntityMetadata m, EntityType entity)
	{
		OrExpression orExp = (OrExpression)logicalExp;
		Expression leftExpression = orExp.getLeftExpression();
		Expression rightExpression = orExp.getRightExpression();

		return new OrFilterBuilder(populateFilterBuilder(leftExpression, m, entity), populateFilterBuilder(rightExpression, m, entity));
	}

	/**
	 * @param conditionalExpression
	 * @return
	 */
	private FilterClause populateFilterClause(ComparisonExpression conditionalExpression)
	{
		String property = ((StateFieldPathExpression)conditionalExpression.getLeftExpression()).getPath(1);
		String condition = conditionalExpression.getComparisonOperator();
		Object value = conditionalExpression.getRightExpression().toParsedText();

		return (condition != null && property != null)? getKunderaQuery().new FilterClause(property, condition, value) : null;
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
		List result = populateEntities(m, client);
		return setRelationEntities(result, client, m);
		// return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.impetus.kundera.query.QueryImpl#getReader()
	 */
	@Override
	protected EntityReader getReader()
	{
		return new ESEntityReader(kunderaQuery, kunderaMetadata);
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

	@Override
	public void close()
	{

	}

	@Override
	public Iterator<E> iterate()
	{
		return null;
	}

	/**
	 * @param clause
	 * @param metadata
	 * @param entityType
	 * @return
	 */
	private FilterBuilder getFilter(FilterClause clause, final EntityMetadata metadata, final EntityType entityType)
	{
		String condition = clause.getCondition();
		Object value = clause.getValue().get(0);
		String name = ((AbstractAttribute)((MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
				metadata.getPersistenceUnit())).entity(metadata.getEntityClazz()).getAttribute(clause.getProperty())).getJPAColumnName();

		FilterBuilder filterBuilder = null;
		if (condition.equals("="))
		{
			filterBuilder = new TermFilterBuilder(name, value);
		}
		else if (condition.equals(">"))
		{
			filterBuilder = new RangeFilterBuilder(name).gt(value);
		}
		else if (condition.equals("<"))
		{
			filterBuilder = new RangeFilterBuilder(name).lt(value);
		}
		else if (condition.equals(">="))
		{
			filterBuilder = new RangeFilterBuilder(name).gte(value);
		}
		else if (condition.equals("<="))
		{
			filterBuilder = new RangeFilterBuilder(name).lte(value);
		}

		return filterBuilder;
	}

	@Override
	protected List findUsingLucene(EntityMetadata m, Client client)
	{
		throw new UnsupportedOperationException("select colummn via lucene is unsupported in couchdb");
	}

	/**
	 * @param query
	 * @param entityMetadata
	 * @param filter
	 * @return
	 */
	public FilterAggregationBuilder useAggregation(KunderaQuery query, EntityMetadata entityMetadata, FilterBuilder filter)
	{
		return (query.getSelectStatement() != null ) ? query.isAggregated() ? buildSelectAggregations(query.getSelectStatement(), entityMetadata, filter) : null : null;
	}

	/**
	 * @param entityMetadata
	 * @param filter
	 * @return
	 */
	private FilterAggregationBuilder buildWhereAggregations(EntityMetadata entityMetadata, FilterBuilder filter)
	{
		filter = filter != null ? filter : FilterBuilders.matchAllFilter();
		FilterAggregationBuilder filteragg = AggregationBuilders.filter("whereClause").filter(filter);
		return filteragg;
	}

	/**
	 * @param selectStatement
	 * @param entityMetadata
	 * @param filter
	 * @return
	 */
	private FilterAggregationBuilder buildSelectAggregations(SelectStatement selectStatement, EntityMetadata entityMetadata, FilterBuilder filter)
	{
		FilterAggregationBuilder filteredAggregation = buildWhereAggregations(entityMetadata, filter); 
		Expression expression = ((SelectClause) selectStatement.getSelectClause()).getSelectExpression();

		if(expression instanceof CollectionExpression)
		{
			filteredAggregation = appendAggregation((CollectionExpression)expression, entityMetadata, filteredAggregation);
		}
		else 
		{
			if(checkExpression(expression))
				filteredAggregation.subAggregation(getAggregation(expression, entityMetadata));
		}
		return filteredAggregation;
	}

	/**
	 * @param collectionExpression
	 * @param entityMetadata
	 * @param aggregationBuilder
	 * @return
	 */
	private FilterAggregationBuilder appendAggregation(CollectionExpression collectionExpression, EntityMetadata entityMetadata, FilterAggregationBuilder aggregationBuilder) {

		ListIterable<Expression> functionlist = collectionExpression.children();
		for(Expression function : functionlist)
		{
			if(checkExpression(function))
				aggregationBuilder.subAggregation(getAggregation(function, entityMetadata));
		}
		return aggregationBuilder;
	}

	/**
	 * @param expression
	 * @param entityMetadata
	 * @return
	 */
	private MetricsAggregationBuilder getAggregation(Expression expression, EntityMetadata entityMetadata) 
	{		
		AggregateFunction function = (AggregateFunction)expression;
		MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
				entityMetadata.getPersistenceUnit());

		String field = function.toParsedText().substring(function.toParsedText().indexOf('.') + 1, function.toParsedText().indexOf(')'));
		String jPAColumnName = ((AbstractAttribute)metaModel.entity(entityMetadata.getEntityClazz()).getAttribute(field)).getJPAColumnName();

		MetricsAggregationBuilder aggregationBuilder = null ;

		switch(function.getIdentifier()){
		case Expression.MIN:
			aggregationBuilder = AggregationBuilders.min(function.toParsedText()).field(jPAColumnName); //AggregationBuilders.min("minimum").field(jPAColumnName);
			break;
		case Expression.MAX:
			aggregationBuilder = AggregationBuilders.max(function.toParsedText()).field(jPAColumnName);
			break;
		case Expression.SUM:
			aggregationBuilder = AggregationBuilders.sum(function.toParsedText()).field(jPAColumnName);
			break;
		case Expression.AVG:
			aggregationBuilder = AggregationBuilders.avg(function.toParsedText()).field(jPAColumnName);
			break;
		}
		return aggregationBuilder;
	}

	/**
	 * @param expression
	 * @return
	 */
	private boolean checkExpression(Expression expression)
	{
		return (expression instanceof StateFieldPathExpression || expression instanceof IdentificationVariable) ? false : true ;
	}
}