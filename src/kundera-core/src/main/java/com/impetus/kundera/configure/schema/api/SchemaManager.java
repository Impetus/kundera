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
package com.impetus.kundera.configure.schema.api;

import java.util.List;

import com.impetus.kundera.configure.schema.TableInfo;

/**
 * Interface to define methods to be implemented by different schema managers.
 * 
 * @author kuldeep.kumar
 * 
 */
public interface SchemaManager
{
    /**
     * Exports schema according to configured schema operation e.g.
     * {create,create-drop,update,validate}
     */
    void exportSchema(String persistenceUnit, List<TableInfo> puToSchemaCol);

    /**
     * Method required to drop auto create schema,in case of schema operation as
     * {create-drop},
     */
    void dropSchema();

    /**
     * validates the entity against the Client specific properties.
     * 
     * @return
     * 
     */
    boolean validateEntity(Class clazz);
}
