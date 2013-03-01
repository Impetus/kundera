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

import java.util.List;

import com.impetus.client.neo4j.Neo4JClient;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Cypher implementation of {@link Neo4JNativeQuery}
 * @author amresh.singh
 */
public class Neo4JCypherQuery implements Neo4JNativeQuery
{

    @Override
    public List<Object> executeNativeQuery(String nativeQuery, Neo4JClient client, EntityMetadata m)
    {
        return null;
    }

}
