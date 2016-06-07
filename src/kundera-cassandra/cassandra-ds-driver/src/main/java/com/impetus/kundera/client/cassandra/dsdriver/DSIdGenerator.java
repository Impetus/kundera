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
package com.impetus.kundera.client.cassandra.dsdriver;

import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.generator.AutoGenerator;

/**
 * The Class DSIdGenerator.
 * 
 * @author: karthikp.manchala
 */
public class DSIdGenerator implements AutoGenerator
{

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

        final String generatedId = "Select now() from system_schema.columns";
        ResultSet rSet = ((DSClient) client).execute(generatedId, null);

        UUID uuid = rSet.iterator().next().getUUID(0);
        return uuid;
    }
}
