/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.cassandra.thrift.cql;

import org.junit.After;
import org.junit.Before;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.countercolumns.SuperCountersTest;
import com.impetus.kundera.PersistenceProperties;

/**
 * 
 * @author Kuldeep.Mishra
 *
 */
public class SuperCountersTestOnCql extends SuperCountersTest
{

    @Before
    public void setUp() throws Exception
    {
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        super.setUp();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }
}
