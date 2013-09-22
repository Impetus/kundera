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
package com.impetus.kundera.loader;

import java.util.Map;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;

/**
 * Interface for client factory.
 * 
 * @author vivek.mishra
 * 
 */
public interface ClientFactory
{

    /**
     * Load.
     * 
     * @param persistenceUnit
     *            the persistence units
     */
    void load(String persistenceUnit, Map<String, Object> puProperties);

    /**
     * Instantiate and returns client instance
     * 
     * @return client instance.
     */
    Client getClientInstance();

    /**
     * return the instance of schema manager
     * 
     * @return schemaManager interface.
     */
    SchemaManager getSchemaManager(Map<String, Object> puProperties);
}
