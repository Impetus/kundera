/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.loader;

/**
 * Interface to define for client life cycle manager.
 * 
 * @author vivek.mishra
 * 
 */
public interface ClientLifeCycleManager
{

    /**
     * Initialize configured client.
     */
    void initialize();

    /**
     * Returns true if client is thread safe, else false.
     * 
     * @return true if client is thread safe.
     */
    boolean isThreadSafe();

    /**
     * Unloads/destroy configured client instance.
     */
    void destroy();
}
