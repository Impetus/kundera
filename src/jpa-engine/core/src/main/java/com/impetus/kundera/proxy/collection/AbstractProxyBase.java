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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.LazyInitializationException;

/**
 * Abstract class containing common methods for all interfaces extending
 * {@link Collection} interface and {@link Map} interface
 * 
 * @author amresh.singh
 */
public abstract class AbstractProxyBase implements ProxyCollection
{

    private PersistenceDelegator delegator;

    private Object owner;

    private Map<String, Object> relationsMap;

    private Relation relation;

    protected Object dataCollection;

    public AbstractProxyBase()
    {
    }

    /**
     * @param delegator
     */
    public AbstractProxyBase(PersistenceDelegator delegator, Relation relation)
    {
        this.delegator = delegator;
        this.relation = relation;
    }

    @Override
    public Object getOwner()
    {
        return owner;
    }

    @Override
    public void setOwner(Object owner)
    {
        this.owner = owner;
    }

    @Override
    public PersistenceDelegator getPersistenceDelegator()
    {
        return delegator;
    }

    @Override
    public Map<String, Object> getRelationsMap()
    {
        return relationsMap;
    }

    @Override
    public void setRelationsMap(Map<String, Object> relationsMap)
    {
        this.relationsMap = relationsMap;
    }

    @Override
    public Relation getRelation()
    {
        return relation;
    }

    /**
 * 
 */
    protected void eagerlyLoadDataCollection()
    {
        if (getDataCollection() == null || getDataCollection() instanceof ProxyCollection)
        {
            EntityMetadata m = KunderaMetadataManager.getEntityMetadata(getPersistenceDelegator().getKunderaMetadata(),
                    getOwner().getClass());

            if (!getPersistenceDelegator().isOpen())
            {
                throw new LazyInitializationException(
                        "Unable to load Proxy Collection."
                                + " This happens when you access a lazily loaded proxy collection in an entity after entity manager has been closed.");
            }
            
            Map<Object, Object> relationStack = new HashMap<Object, Object>();
            getPersistenceDelegator().getClient(m).getReader()
                    .recursivelyFindEntities(getOwner(), relationsMap, m, getPersistenceDelegator(), true,relationStack);

            if (getRelation().getProperty().getType().isAssignableFrom(Map.class))
            {
                dataCollection = (Map) PropertyAccessorHelper.getObject(getOwner(), getRelation().getProperty());
            }
            else
            {
                dataCollection = (Collection) PropertyAccessorHelper.getObject(getOwner(), getRelation().getProperty());
            }

            if (dataCollection instanceof ProxyCollection)
            {
                if (getRelation().getProperty().getType().isAssignableFrom(java.util.Set.class))
                {
                    dataCollection = new HashSet();
                }
                else if (getRelation().getProperty().getType().isAssignableFrom(java.util.List.class))
                {
                    dataCollection = new ArrayList();
                }
                else
                {
                    dataCollection = null;
                }
            }
            PropertyAccessorHelper.set(getOwner(), getRelation().getProperty(), dataCollection);
        }
    }

    // ///////////////////Common collection
    // implementation////////////////////////////

}