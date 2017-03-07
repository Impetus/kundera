/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.client.kudu;

import java.util.UUID;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.generator.AutoGenerator;

/**
 * The Class KuduDBIdGenerator.
 * 
 * @author karthikp.manchala
 */
public class KuduDBIdGenerator implements AutoGenerator
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
        return UUID.randomUUID().toString();
    }
}
