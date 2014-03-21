/**
 * 
 */
package com.impetus.kundera.persistence;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.ParameterExpression;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.EntityType;


/**
 * @author vivek.mishra
 *
 */
public class KunderaCritieriaQuery<T> implements CriteriaQuery<T>
{

    private KunderaCriteriaBuilder criteriaBuilder;
    private Class<T> returnType;
    private QueryPlan queryPlan = new QueryPlan();
    
    KunderaCritieriaQuery(KunderaCriteriaBuilder kunderaCriteriaBuilder)
    {
        this.criteriaBuilder = kunderaCriteriaBuilder;
        this.returnType = (Class<T>) Object.class;
    }

    KunderaCritieriaQuery(KunderaCriteriaBuilder kunderaCriteriaBuilder, Class<T> returnClazz)
    {
        this.criteriaBuilder = kunderaCriteriaBuilder;
        this.returnType = returnClazz;
    }

    @Override
    public <X> Root<X> from(Class<X> paramClass)
    {
        EntityType<X> entityType = this.criteriaBuilder.getMetaModel().entity(paramClass);
        return fromEntityType(entityType);
    }

    
    @Override
    public <X> Root<X> from(EntityType<X> paramEntityType)
    {
        
        if(!this.criteriaBuilder.getMetaModel().getEntities().contains(paramEntityType))
        {
            throw new IllegalArgumentException("Invalid entity type, {class:" + paramEntityType.getName() + "}");   
        }
        
        return fromEntityType(paramEntityType);
    }

    @Override
    public <U> Subquery<U> subquery(Class<U> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<Root<?>> getRoots()
    {
        return this.queryPlan.from;
    }

    @Override
    public Selection<T> getSelection()
    {
        return (Selection<T>) this.queryPlan.select;
    }

    @Override
    public Predicate getRestriction()
    {
        return this.queryPlan.where;
    }

    @Override
    public List<Expression<?>> getGroupList()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Predicate getGroupRestriction()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isDistinct()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Class<T> getResultType()
    {
        return this.returnType;
    }

    @Override
    public CriteriaQuery<T> select(Selection<? extends T> paramSelection)
    {
        this.queryPlan.setSelection(paramSelection);
        return this;
    }

    @Override
    public CriteriaQuery<T> multiselect(Selection<?>... paramArrayOfSelection)
    {
        SelectionType type = SelectionType.getSelectionType(getResultType());
        switch (type)
        {
        case TUPLE:
                this.queryPlan.setSelection(this.criteriaBuilder.tuple(paramArrayOfSelection));
            break;

        case ARRAY:
            this.queryPlan.setSelection(this.criteriaBuilder.array(this.getResultType(),paramArrayOfSelection));
            break;

        case OBJECT:
            this.queryPlan.setSelection(this.criteriaBuilder.construct(getResultType(), paramArrayOfSelection));
            break;

        }

        return this;
    }

    @Override
    public CriteriaQuery<T> multiselect(List<Selection<?>> paramList)
    {
//        this.queryPlan
        return multiselect((Selection<?>[]) paramList.toArray());
    }

    @Override
    public CriteriaQuery<T> where(Expression<Boolean> paramExpression)
    {
        if (Predicate.class.isAssignableFrom(paramExpression.getClass()))
        {
            this.queryPlan.setWhere(this.criteriaBuilder.and((Predicate)paramExpression));
        }
        
        return this;
    }

    @Override
    public CriteriaQuery<T> where(Predicate... paramArrayOfPredicate)
    {
        this.queryPlan.setWhere(this.criteriaBuilder.and(paramArrayOfPredicate));
        return this;
    }

    @Override
    public CriteriaQuery<T> groupBy(Expression<?>... paramArrayOfExpression)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CriteriaQuery<T> groupBy(List<Expression<?>> paramList)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CriteriaQuery<T> having(Expression<Boolean> paramExpression)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CriteriaQuery<T> having(Predicate... paramArrayOfPredicate)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CriteriaQuery<T> orderBy(Order... paramArrayOfOrder)
    {
        return orderBy(Arrays.asList(paramArrayOfOrder));
    }

    @Override
    public CriteriaQuery<T> orderBy(List<Order> paramList)
    {
        // TODO Auto-generated method stub
        this.queryPlan.setOrderBy(paramList);
        return this;
    }

    @Override
    public CriteriaQuery<T> distinct(boolean paramBoolean)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Order> getOrderList()
    {
        return this.queryPlan.orderBy;
    }

    @Override
    public Set<ParameterExpression<?>> getParameters()
    {
        // TODO Auto-generated method stub
        return null;
    }

    private <X> Root<X> fromEntityType(EntityType<X> paramEntityType)
    {
        Root<X> root =  new DefaultRoot<X>(paramEntityType);
        this.queryPlan.add(root);
        return root;
    }

    enum SelectionType
    {
        TUPLE, ARRAY, OBJECT;
        
        static SelectionType getSelectionType(Class clazz)
        {
            if(clazz.isAssignableFrom(Tuple.class))
            {
                return TUPLE;
            }else if(clazz.isArray())
            {
                return ARRAY;
            }else
            {
                return OBJECT;
            }
        }
    }
    
    class QueryPlan
    {
        private Selection<?> select;
        private Set<Root<?>> from = new HashSet<Root<?>>();
        private Predicate where;
        private List<Order> orderBy;
     
        void add(Root root)
        {
            from.add(root);
        }
        
        void setWhere(Predicate predicate)
        {
            this.where = predicate;
        }
        
        void setSelection(Selection selection)
        {
            this.select = selection;
        }
        
        
        void setOrderBy(List<Order> orderBy)
        {
            this.orderBy = orderBy;
        }
    }
}
