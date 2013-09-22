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

import javax.persistence.spi.LoadState;
import javax.persistence.spi.ProviderUtil;

/**
 * {@link ProviderUtil} for {@link KunderaPersistence}
 * 
 * @author amresh.singh
 */
public class KunderaPersistenceProviderUtil implements ProviderUtil
{
    KunderaPersistence provider;

    public KunderaPersistenceProviderUtil(KunderaPersistence provider)
    {
        this.provider = provider;
    }

    /**
     * If the provider determines that the entity has been provided by itself
     * and that the state of the specified attribute has been loaded, this
     * method returns LoadState.LOADED. If the provider determines that the
     * entity has been provided by itself and that either entity attributes with
     * FetchType EAGER have not been loaded or that the state of the specified
     * attribute has not been loaded, this methods returns LoadState.NOT_LOADED.
     * If a provider cannot determine the load state, this method returns
     * LoadState.UNKNOWN. The provider's implementation of this method must not
     * obtain a reference to an attribute value, as this could trigger the
     * loading of entity state if the entity has been provided by a different
     * provider.
     * 
     * @param entity
     * @param attributeName
     *            name of attribute whose load status is to be determined
     * @return load status of the attribute
     */
    @Override
    public LoadState isLoadedWithoutReference(Object paramObject, String paramString)
    {
        return PersistenceUtilHelper.isLoadedWithoutReference(paramObject, paramString, provider.getCache());
    }

    /**
     * If the provider determines that the entity has been provided by itself
     * and that the state of the specified attribute has been loaded, this
     * method returns LoadState.LOADED. If a provider determines that the entity
     * has been provided by itself and that either the entity attributes with
     * FetchType EAGER have not been loaded or that the state of the specified
     * attribute has not been loaded, this method returns LoadState.NOT_LOADED.
     * If the provider cannot determine the load state, this method returns
     * LoadState.UNKNOWN. The provider's implementation of this method is
     * permitted to obtain a reference to the attribute value. (This access is
     * safe because providers which might trigger the loading of the attribute
     * state will have already been determined by isLoadedWithoutReference. )
     * 
     * @param entity
     * @param attributeName
     *            name of attribute whose load status is to be determined
     * @return load status of the attribute
     */
    @Override
    public LoadState isLoadedWithReference(Object paramObject, String paramString)
    {
        return PersistenceUtilHelper.isLoadedWithReference(paramObject, paramString, provider.getCache());
    }

    /**
     * If the provider determines that the entity has been provided by itself
     * and that the state of all attributes for which FetchType EAGER has been
     * specified have been loaded, this method returns LoadState.LOADED. If the
     * provider determines that the entity has been provided by itself and that
     * not all attributes with FetchType EAGER have been loaded, this method
     * returns LoadState.NOT_LOADED. If the provider cannot determine if the
     * entity has been provided by itself, this method returns
     * LoadState.UNKNOWN. The provider's implementation of this method must not
     * obtain a reference to any attribute value, as this could trigger the
     * loading of entity state if the entity has been provided by a different
     * provider.
     * 
     * @param entity
     *            whose loaded status is to be determined
     * @return load status of the entity
     */
    @Override
    public LoadState isLoaded(Object paramObject)
    {
        return PersistenceUtilHelper.isLoaded(paramObject);
    }

}
