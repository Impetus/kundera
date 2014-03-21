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

import javax.persistence.criteria.Fetch;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SingularAttribute;

/**
 * Default implementation for {@link Root}
 * @author vivek.mishra
 * 
 */
public class DefaultRoot<X> extends DefaultFrom<X, X> implements Root<X>
{


    DefaultRoot(EntityType<X> entityType)
    {
        this.entityType = entityType;
        this.managedType = entityType;
    }


    @Override
    public Set<Fetch<X, ?>> getFetches()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <Y> Fetch<X, Y> fetch(SingularAttribute<? super X, Y> paramSingularAttribute)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <Y> Fetch<X, Y> fetch(SingularAttribute<? super X, Y> paramSingularAttribute, JoinType paramJoinType)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <Y> Fetch<X, Y> fetch(PluralAttribute<? super X, ?, Y> paramPluralAttribute)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <Y> Fetch<X, Y> fetch(PluralAttribute<? super X, ?, Y> paramPluralAttribute, JoinType paramJoinType)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <X, Y> Fetch<X, Y> fetch(String paramString)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <X, Y> Fetch<X, Y> fetch(String paramString, JoinType paramJoinType)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.persistence.DefaultPath#getModel()
     */
    @Override
    public EntityType<X> getModel()
    {
        return this.entityType;
    }
}
