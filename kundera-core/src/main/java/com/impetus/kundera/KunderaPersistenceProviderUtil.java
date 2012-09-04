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
 * @author amresh.singh
 */
public class KunderaPersistenceProviderUtil implements ProviderUtil
{

    @Override
    public LoadState isLoadedWithoutReference(Object paramObject, String paramString)
    {
        return null;
    }

    @Override
    public LoadState isLoadedWithReference(Object paramObject, String paramString)
    {
        return null;
    }

    @Override
    public LoadState isLoaded(Object paramObject)
    {
        return null;
    }    

}
