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
package com.impetus.kundera.generator;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;

/**
 * {@link IdentityGenerator} interface , all client should implement this
 * interface in order to support identity generation strategy.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public interface IdentityGenerator extends Generator
{

    /**
     * Generate.
     * 
     * @param discriptor
     *            the discriptor
     * @param client
     *            the client
     * @param dataType
     *            the data type
     * @return the object
     */
    public Object generate(SequenceGeneratorDiscriptor discriptor, Client<?> client, String dataType);
}
