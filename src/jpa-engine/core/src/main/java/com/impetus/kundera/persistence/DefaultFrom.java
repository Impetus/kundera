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

import java.util.Set;

import javax.persistence.criteria.CollectionJoin;
import javax.persistence.criteria.Fetch;
import javax.persistence.criteria.From;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.ListJoin;
import javax.persistence.criteria.MapJoin;
import javax.persistence.criteria.SetJoin;
import javax.persistence.metamodel.CollectionAttribute;
import javax.persistence.metamodel.ListAttribute;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;

/**
 * Default implementation class for {@link From}
 * 
 * @author vivek.mishra
 *
 */
public class DefaultFrom<Z,X> extends DefaultPath<X> implements From<Z, X>
{

    public DefaultFrom()
    {
     super();
    }
    @Override
    public Set<Fetch<X, ?>> getFetches()
    {
        throw new UnsupportedOperationException("Method getFetches() is not yet supported");
    }

    @Override
    public <Y> Fetch<X, Y> fetch(SingularAttribute<? super X, Y> paramSingularAttribute)
    {
        throw new UnsupportedOperationException("Method fetch(SingularAttribute<? super X, Y> paramSingularAttribute) is not yet supported");
    }

    @Override
    public <Y> Fetch<X, Y> fetch(SingularAttribute<? super X, Y> paramSingularAttribute, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method fetch(SingularAttribute<? super X, Y> paramSingularAttribute, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <Y> Fetch<X, Y> fetch(PluralAttribute<? super X, ?, Y> paramPluralAttribute)
    {
        throw new UnsupportedOperationException("Method fetch(PluralAttribute<? super X, ?, Y> paramPluralAttribute) is not yet supported");
    }

    @Override
    public <Y> Fetch<X, Y> fetch(PluralAttribute<? super X, ?, Y> paramPluralAttribute, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method fetch(PluralAttribute<? super X, ?, Y> paramPluralAttribute, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <X, Y> Fetch<X, Y> fetch(String paramString)
    {
        throw new UnsupportedOperationException("Method fetch(String paramString) is not yet supported");
    }

    @Override
    public <X, Y> Fetch<X, Y> fetch(String paramString, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method fetch(String paramString, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public Set<Join<X, ?>> getJoins()
    {
        throw new UnsupportedOperationException("Method getJoins() is not yet supported");
    }

    @Override
    public boolean isCorrelated()
    {
        throw new UnsupportedOperationException("Method isCorrelated() is not yet supported");
    }

    @Override
    public From<Z, X> getCorrelationParent()
    {
        throw new UnsupportedOperationException("Method getCorrelationParent() is not yet supported");
    }

    @Override
    public <Y> Join<X, Y> join(SingularAttribute<? super X, Y> paramSingularAttribute)
    {
        throw new UnsupportedOperationException("Method join(SingularAttribute<? super X, Y> paramSingularAttribute) is not yet supported");
    }

    @Override
    public <Y> Join<X, Y> join(SingularAttribute<? super X, Y> paramSingularAttribute, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method join(SingularAttribute<? super X, Y> paramSingularAttribute, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <Y> CollectionJoin<X, Y> join(CollectionAttribute<? super X, Y> paramCollectionAttribute)
    {
        throw new UnsupportedOperationException("Method join(CollectionAttribute<? super X, Y> paramCollectionAttribute) is not yet supported");
    }

    @Override
    public <Y> SetJoin<X, Y> join(SetAttribute<? super X, Y> paramSetAttribute)
    {
        throw new UnsupportedOperationException("Method join(SetAttribute<? super X, Y> paramSetAttribute) is not yet supported");
    }

    @Override
    public <Y> ListJoin<X, Y> join(ListAttribute<? super X, Y> paramListAttribute)
    {
        throw new UnsupportedOperationException("Method join(ListAttribute<? super X, Y> paramListAttribute) is not yet supported");
    }

    @Override
    public <K, V> MapJoin<X, K, V> join(MapAttribute<? super X, K, V> paramMapAttribute)
    {
        throw new UnsupportedOperationException("Method join(MapAttribute<? super X, K, V> paramMapAttribute) is not yet supported");
    }

    @Override
    public <Y> CollectionJoin<X, Y> join(CollectionAttribute<? super X, Y> paramCollectionAttribute,
            JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method join(CollectionAttribute<? super X, Y> paramCollectionAttribute,JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <Y> SetJoin<X, Y> join(SetAttribute<? super X, Y> paramSetAttribute, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method join(SetAttribute<? super X, Y> paramSetAttribute, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <Y> ListJoin<X, Y> join(ListAttribute<? super X, Y> paramListAttribute, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method join(ListAttribute<? super X, Y> paramListAttribute, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <K, V> MapJoin<X, K, V> join(MapAttribute<? super X, K, V> paramMapAttribute, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method join(MapAttribute<? super X, K, V> paramMapAttribute, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <X, Y> Join<X, Y> join(String paramString)
    {
        throw new UnsupportedOperationException("Method join(String paramString) is not yet supported");
    }

    @Override
    public <X, Y> CollectionJoin<X, Y> joinCollection(String paramString)
    {
        throw new UnsupportedOperationException("Method joinCollection(String paramString) is not yet supported");
    }

    @Override
    public <X, Y> SetJoin<X, Y> joinSet(String paramString)
    {
        throw new UnsupportedOperationException("Method joinSet(String paramString) is not yet supported");
    }

    @Override
    public <X, Y> ListJoin<X, Y> joinList(String paramString)
    {
        throw new UnsupportedOperationException("Method joinList(String paramString) is not yet supported");
    }

    @Override
    public <X, K, V> MapJoin<X, K, V> joinMap(String paramString)
    {
        throw new UnsupportedOperationException("Method joinMap(String paramString) is not yet supported");
    }

    @Override
    public <X, Y> Join<X, Y> join(String paramString, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method join(String paramString, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <X, Y> CollectionJoin<X, Y> joinCollection(String paramString, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method joinCollection(String paramString, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <X, Y> SetJoin<X, Y> joinSet(String paramString, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method joinSet(String paramString, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <X, Y> ListJoin<X, Y> joinList(String paramString, JoinType paramJoinType)
    {
        throw new UnsupportedOperationException("Method joinList(String paramString, JoinType paramJoinType) is not yet supported");
    }

    @Override
    public <X, K, V> MapJoin<X, K, V> joinMap(String paramString, JoinType paramJoinType)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
