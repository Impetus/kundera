/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.proxy.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Proxy class used to represent instances for {@link Set}
 * 
 * @author amresh.singh
 */
public class ProxySet extends AbstractProxyCollection implements ProxyCollection, Set
{

    /**
     * Default constructor
     */
    public ProxySet()
    {
        super();
    }

    public ProxySet(final PersistenceDelegator delegator, final Relation relation)
    {
        super(delegator, relation);
    }

    @Override
    public ProxyCollection getCopy()
    {
        ProxyCollection proxyCollection = new ProxySet(getPersistenceDelegator(), getRelation());
        proxyCollection.setRelationsMap(getRelationsMap());
        return proxyCollection;
    }

    // ///////////Methods from Collection interface////////////////////////////
    @Override
    public void clear()
    {
        super.clear();
    }

    @Override
    public boolean contains(final Object arg0)
    {
        return super.contains(arg0);
    }

    @Override
    public boolean containsAll(final Collection arg0)
    {
        return super.containsAll(arg0);
    }

    @Override
    public boolean isEmpty()
    {
        return super.isEmpty();
    }

    @Override
    public int size()
    {
        return super.size();
    }

    @Override
    public boolean add(final Object object)
    {
        return super.add(object);
    }

    @Override
    public boolean addAll(final Collection collection)
    {
        return super.addAll(collection);
    }

    @Override
    public boolean remove(final Object object)
    {
        return super.remove(object);
    }

    @Override
    public boolean removeAll(final Collection collection)
    {
        return super.removeAll(collection);
    }

    @Override
    public boolean retainAll(final Collection collection)
    {
        return super.retainAll(collection);
    }

    @Override
    public Iterator iterator()
    {
        return super.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return super.toArray();
    }

    @Override
    public Object[] toArray(final Object[] arg0)
    {
        return super.toArray(arg0);
    }
}
