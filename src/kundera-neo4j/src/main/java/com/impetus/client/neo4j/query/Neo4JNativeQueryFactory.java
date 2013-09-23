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
package com.impetus.client.neo4j.query;

import com.impetus.kundera.query.QueryHandlerException;

/**
 * factory for {@link Neo4JNativeQuery}
 * 
 * @author amresh.singh
 */
public class Neo4JNativeQueryFactory
{
    public static Neo4JNativeQuery getNativeQueryImplementation(Neo4JQueryType queryType)
    {
        if (queryType.equals(Neo4JQueryType.LUCENE))
        {
            return new Neo4JLuceneQuery();
        }
        /*
         * else if (queryType.equals(Neo4JQueryType.CYPHER)) { return new
         * Neo4JCypherQuery(); }
         */
        /*
         * else if (queryType.equals(Neo4JQueryType.GREMLIN)) {
         * 
         * }
         */
        else
        {
            throw new QueryHandlerException("Invalid Query Type:" + queryType
                    + ".Can't determine and implementation for running this type for native query for Neo4J");
        }
    }
}
