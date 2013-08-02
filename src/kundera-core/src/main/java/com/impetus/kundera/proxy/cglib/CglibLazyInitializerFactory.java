/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.proxy.cglib;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Entity;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.LazyInitializerFactory;

/**
 * Implementation of LazyInitializerFactory using cglib library.
 * 
 * @author animesh.kumar
 */
public class CglibLazyInitializerFactory implements LazyInitializerFactory
{

    Map<String, KunderaProxy> proxies = new HashMap<String, KunderaProxy>();

    @Override
    public KunderaProxy getProxy(String entityName, Class<?> persistentClass, Method getIdentifierMethod,
            Method setIdentifierMethod, Object id, PersistenceDelegator pd)
    {
        KunderaProxy kunderaProxy = (KunderaProxy) CglibLazyInitializer.getProxy(entityName, persistentClass,
                new Class[] { KunderaProxy.class }, getIdentifierMethod, setIdentifierMethod, id, pd);
        proxies.put(entityName, kunderaProxy);

        return kunderaProxy;
    }

    @Override
    public KunderaProxy getProxy(String entityName)
    {
        return proxies.get(entityName);
    }

    @Override
    public void clearProxies()
    {
        for (KunderaProxy proxy : proxies.values())
        {
            proxy.getKunderaLazyInitializer().setOwner(null);
            proxy.getKunderaLazyInitializer().setInitialized(false);
        }
        proxies.clear();
    }
    
 
    @Override
    public <E> void setProxyOwners(EntityMetadata entityMetadata, E e)
    {
        if (e != null && e.getClass().getAnnotation(Entity.class) != null && entityMetadata != null)
        {
            for (Relation r : entityMetadata.getRelations())
            {
                Object entityId = PropertyAccessorHelper.getId(e, entityMetadata);
                if (r.isUnary())
                {
                    String entityName = entityMetadata.getEntityClazz().getName() + "_" + entityId + "#"
                            + r.getProperty().getName();

                    KunderaProxy kunderaProxy = getProxy(entityName);
                    if (kunderaProxy != null)
                    {
                        kunderaProxy.getKunderaLazyInitializer().setOwner(e);
                    }
                }
            }
        }
    }
}
