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
package com.impetus.kundera.metadata.model;

import com.impetus.kundera.proxy.LazyInitializerFactory;

/**
 * The Class CoreMetadata.
 * 
 * @author amresh.singh
 */
public class CoreMetadata
{

    /** The lazy initializer factory. */
    private LazyInitializerFactory lazyInitializerFactory;
   

    /**
     * Gets the lazy initializer factory.
     * 
     * @return the lazy initializer factory
     */
    public LazyInitializerFactory getLazyInitializerFactory()
    {
        return lazyInitializerFactory;
    }

    /**
     * Sets the lazy initializer factory.
     * 
     * @param lazyInitializerFactory
     *            the new lazy initializer factory
     */
    public void setLazyInitializerFactory(LazyInitializerFactory lazyInitializerFactory)
    {
        this.lazyInitializerFactory = lazyInitializerFactory;
    }

}
