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
package com.impetus.kundera.service.policy;

import java.util.LinkedHashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * To test round robin algo.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class RoundRobinBalancingPolicyTest
{

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.service.policy.RoundRobinBalancingPolicy#getPool(java.util.Collection)}
     * .
     */
    @Test
    public void testGetPool()
    {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        map.put("first", o1);
        map.put("second", o2);
        map.put("third", o3);

        RoundRobinBalancingPolicy balancingPolicy = new RoundRobinBalancingPolicy();

        for (int i = 0; i < 1300; i++)
        {
            Object o = balancingPolicy.getPool(map.values());
            Assert.assertNotNull(o);
            if (i % 3 == 0)
            {
                Assert.assertEquals(o1, o);
            }
            else if (i % 3 == 1)
            {
                Assert.assertEquals(o2, o);
            }
            else if (i % 3 == 2)
            {
                Assert.assertEquals(o3, o);
            }
        }
    }
}
