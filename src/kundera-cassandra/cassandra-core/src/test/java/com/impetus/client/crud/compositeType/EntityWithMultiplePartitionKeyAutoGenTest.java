/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.crud.compositeType;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.Persistence;

import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;

/**
 * The Class EntityWithMultiplePartitionKeyAutoGenTest.
 *
 * @author karthikp.manchala
 */
public class EntityWithMultiplePartitionKeyAutoGenTest extends EntityWithMultiplePartitionKeyTest
{
    
    /* (non-Javadoc)
     * @see com.impetus.client.crud.compositeType.EntityWithMultiplePartitionKeyTest#setUp()
     */
    @Before
    public void setUp() throws Exception
    {
        Map<String, String> propertymap = new HashMap<String, String>();
        propertymap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        propertymap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        emf = Persistence.createEntityManagerFactory(_PU, propertymap);
    }
}