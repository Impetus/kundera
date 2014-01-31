/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera;

import javax.persistence.PersistenceUnitUtil;
import javax.persistence.spi.LoadState;

import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * {@link PersistenceUnitUtil} for {@link KunderaPersistence}
 * 
 * @author amresh.singh
 */
public class KunderaPersistenceUnitUtil implements PersistenceUnitUtil
{
    private transient PersistenceUtilHelper.MetadataCache cache;

    public KunderaPersistenceUnitUtil(PersistenceUtilHelper.MetadataCache cache)
    {
        this.cache = cache;
    }

    @Override
    public boolean isLoaded(Object entity, String attributeName)
    {
        LoadState state = PersistenceUtilHelper.isLoadedWithoutReference(entity, attributeName, this.cache);
        if (state == LoadState.LOADED)
        {
            return true;
        }
        if (state == LoadState.NOT_LOADED)
        {
            return false;
        }
        return (PersistenceUtilHelper.isLoadedWithReference(entity, attributeName, this.cache) != LoadState.NOT_LOADED);
    }

    @Override
    public boolean isLoaded(Object entity)
    {
        return (PersistenceUtilHelper.isLoaded(entity) != LoadState.NOT_LOADED);
    }

    @Override
    public Object getIdentifier(Object entity)
    {
        Class<?> entityClass = entity.getClass();
//        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
//
//        if (entityMetadata == null)
//        {
//            throw new IllegalArgumentException(entityClass + " is not an entity");
//        }
        return PropertyAccessorHelper.getId(entity, /*entityMetadata*/null);
    }

}
