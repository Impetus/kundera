/**
 * 
 */
package com.impetus.kundera.persistence;

import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Order;

import com.impetus.kundera.query.KunderaQuery.SortOrder;

/**
 * @author vivek.mishra
 *
 */
public class QueryOrder implements Order
{

    private Expression<?> expression;
    private SortOrder ordering;
    
    QueryOrder(final Expression<?> expression, SortOrder sortOrdering)
    {
        this.expression = expression;
        this.ordering = sortOrdering;
    }
    
    /* (non-Javadoc)
     * @see javax.persistence.criteria.Order#reverse()
     */
    @Override
    public Order reverse()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.persistence.criteria.Order#isAscending()
     */
    @Override
    public boolean isAscending()
    {
        // TODO Auto-generated method stub
        return this.ordering.equals(SortOrder.ASC);
    }

    SortOrder getOrder()
    {
        return this.ordering;
    }
    
    /* (non-Javadoc)
     * @see javax.persistence.criteria.Order#getExpression()
     */
    @Override
    public Expression<?> getExpression()
    {
        return this.expression;
    }

}
