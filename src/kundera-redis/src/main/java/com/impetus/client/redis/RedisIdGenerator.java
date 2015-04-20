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
package com.impetus.client.redis;

import redis.clients.jedis.Jedis;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.generator.SequenceGenerator;
import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;

/**
 * The Class RedisIdGenerator.
 * 
 * @author: karthikp.manchala
 */
public class RedisIdGenerator implements SequenceGenerator
{

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
        Jedis jedis = ((RedisClient) client).factory.getConnection();

        Long latestCount = jedis.incr(((RedisClient) client).getEncodedBytes(discriptor.getSequenceName()));
        if (latestCount == 1)
        {
            return discriptor.getInitialValue();
        }
        else
        {
            return (latestCount - 1) * discriptor.getAllocationSize();
        }
    }

}
