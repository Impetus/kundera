/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.kundera.client;

import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.IdentityGenerator;
import com.impetus.kundera.generator.SequenceGenerator;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;

/**
 * The Class CoreTestIdGenerator.
 * 
 * @author: karthikp.manchala
 * 
 */
public class CoreTestIdGenerator implements AutoGenerator, TableGenerator, SequenceGenerator, IdentityGenerator
{

    /** The id count. */
    private static int idCount;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.TableGenerator#generate(com.impetus.kundera
     * .metadata.model.TableGeneratorDiscriptor,
     * com.impetus.kundera.client.ClientBase, java.lang.Object)
     */
    @Override
    public Object generate(TableGeneratorDiscriptor discriptor, ClientBase client, String dataType)
    {
        return ++idCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.AutoGenerator#generate(com.impetus.kundera
     * .client.Client, java.lang.Object)
     */
    @Override
    public Object generate(Client<?> client, String dataType)
    {
        return ++idCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.SequenceGenerator#generate(com.impetus.
     * kundera.metadata.model.SequenceGeneratorDiscriptor,
     * com.impetus.kundera.client.Client, java.lang.Object)
     */
    @Override
    public Object generate(SequenceGeneratorDiscriptor discriptor, Client<?> client, String dataType)
    {
        return ++idCount;
    }

}
