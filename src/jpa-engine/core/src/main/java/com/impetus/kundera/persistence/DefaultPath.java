/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Selection;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.Bindable;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SingularAttribute;

import com.impetus.kundera.metadata.model.attributes.DefaultSingularAttribute;
import com.impetus.kundera.metadata.model.type.DefaultEmbeddableType;

/**
 * Default implementation for {@link Path}
 * 
 * @author vivek.mishra
 * 
 */
public class DefaultPath<X> implements Path<X>
{

    private PathType pathType;

    protected ManagedType<X> managedType;

    private Attribute<X, ?> attribute;
    
    private Attribute embeddedAttribute;
    
    protected EntityType<X> entityType;

    private String alias;

    // protected EntityType<X> entityType;

    private PathCache cache = new PathCache();

    DefaultPath()
    {

    }

    private DefaultPath(PathType pathType, ManagedType<X> managedType, Attribute<X, ?> attribute, EntityType<X> entityType,Attribute embeddedAttribute)
    {
        this.pathType = pathType;
        this.managedType = managedType;
        this.attribute = attribute;
        this.entityType = entityType;
        if(embeddedAttribute != null && embeddedAttribute.getPersistentAttributeType().equals(PersistentAttributeType.EMBEDDED))
        {
            this.embeddedAttribute =embeddedAttribute;
        }
    }

    @Override
    public <Y> Path<Y> get(SingularAttribute<? super X, Y> paramSingularAttribute)
    {

        return cache.get(paramSingularAttribute,this.entityType, this.attribute);
    }

    @Override
    public <E, C extends Collection<E>> Expression<C> get(PluralAttribute<X, C, E> paramPluralAttribute)
    {
        // return cache.get(paramPluralAttribute);
        throw new UnsupportedOperationException("Support for plural attribute is not yet available");
    }

    @Override
    public <K, V, M extends Map<K, V>> Expression<M> get(MapAttribute<X, K, V> paramMapAttribute)
    {
        throw new UnsupportedOperationException("Support for map attribute is not yet available");
    }

    @Override
    public Expression<Class<? extends X>> type()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.criteria.Path#get(java.lang.String)
     */
    @Override
    public <Y> Path<Y> get(String paramString)
    {
        Attribute attribute = null;
        if (this.attribute != null
                && this.attribute.getPersistentAttributeType().equals(PersistentAttributeType.EMBEDDED))
        {
            try
            {
                DefaultEmbeddableType embeddableType = (DefaultEmbeddableType) ((DefaultSingularAttribute) this.attribute)
                        .getType();
                attribute = embeddableType.getAttribute(paramString);
            }
            catch (IllegalArgumentException iaex)
            {
                // do nothing. ignore
            }
        }

        attribute = attribute == null ? this.managedType.getAttribute(paramString) : attribute;

        // TODO:: need to check for illegalStateException.

        return cache.get(attribute, this.entityType, this.attribute);
    }

    @Override
    public Predicate isNull()
    {
        throw new UnsupportedOperationException("Method isNull() not yet supported");
    }

    @Override
    public Predicate isNotNull()
    {
        throw new UnsupportedOperationException("Method isNotNull() not yet supported");
    }

    @Override
    public Predicate in(Object... paramArrayOfObject)
    {
        throw new UnsupportedOperationException("Method in(Object... paramArrayOfObject) not yet supported");
    }

    @Override
    public Predicate in(Expression<?>... paramArrayOfExpression)
    {
        throw new UnsupportedOperationException("Method in(Expression<?>... paramArrayOfExpression) not yet supported");
    }

    @Override
    public Predicate in(Collection<?> paramCollection)
    {
        throw new UnsupportedOperationException("Method in(Collection<?> paramCollection) not yet supported");
    }

    @Override
    public Predicate in(Expression<Collection<?>> paramExpression)
    {
        throw new UnsupportedOperationException("Method in(Expression<Collection<?>> paramExpression) not yet supported");
    }

    @Override
    public <X> Expression<X> as(Class<X> paramClass)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Selection<X> alias(String alias)
    {
        this.alias = alias;
        return this;
    }

    @Override
    public boolean isCompoundSelection()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<Selection<?>> getCompoundSelectionItems()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<? extends X> getJavaType()
    {
        return this.managedType.getJavaType();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.TupleElement#getAlias()
     */
    @Override
    public String getAlias()
    {
        return this.alias;
    }

    public EntityType<X> getEntityType()
    {
        return this.entityType;
    }
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.persistence.DefaultPath#getModel()
     * 
     * @Override public EntityType<X> getModel() { return this.managedType; }
     */
    enum PathType
    {
        SINGULAR, PLURAL;
    }

    Attribute getAttribute()
    {
        return this.attribute;
    }

    ManagedType<X> getManagedType()
    {
        return managedType;
    }
    
    Attribute getEmbeddedAttribute()
    {
        return embeddedAttribute;
    }
    
    private <Y> Path<Y> getPath(Attribute attribute,EntityType<Y> entityType,Attribute embeddedAttribute)
    {
        Path<Y> path = null;
        if (attribute.isCollection())
        {
            path = new DefaultPath<Y>(PathType.PLURAL, attribute.getDeclaringType(), attribute, entityType,embeddedAttribute);
        }
        else
        {
            path = new DefaultPath<Y>(PathType.SINGULAR, attribute.getDeclaringType(), attribute,entityType,embeddedAttribute);
        }

        return path;
    }

    @Override
    public Bindable<X> getModel()
    {
        return (Bindable<X>) this.attribute;
    }

    @Override
    public Path<?> getParentPath()
    {
        // TODO Auto-generated method stub
        return null;
    }

    class PathCache
    {
        private Map<String, Path> cacheAttributes = new ConcurrentHashMap<String, Path>();

        synchronized Path get(Attribute attribute, EntityType<X> entityType, Attribute embeddedAttribute)
        {
            if (!cacheAttributes.containsKey(attribute.getName()))
            {
                cacheAttributes.put(attribute.getName(), getPath(attribute,entityType,embeddedAttribute));
            }
            return cacheAttributes.get(attribute.getName());
        }

    }
}
