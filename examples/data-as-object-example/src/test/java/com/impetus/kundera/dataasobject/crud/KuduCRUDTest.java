/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera.dataasobject.crud;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.dataasobject.entities.Customer;

import junit.framework.Assert;

/**
 * The Class KuduCRUDTest.
 */
public class KuduCRUDTest
{

    /**
     * Sets the up before class.
     */
    @BeforeClass
    public static void SetUpBeforeClass()
    {
        Customer.bind("client-properties.json", Customer.class);
    }

    /**
     * Test crud.
     */
    @Test
    public void testCRUD()
    {
        testInsert();
        testUpdate();
        testDelete();
    }

    /**
     * Test insert.
     */
    private void testInsert()
    {
        Customer customer = new Customer();
        customer.setCustomerId(101);
        customer.setName("dev");
        customer.setLocation("Noida");
        customer.save();

        Customer c = new Customer().find(101);

        Assert.assertEquals(101, c.getCustomerId());
        Assert.assertEquals("dev", c.getName());
        Assert.assertEquals("Noida", c.getLocation());
    }

    /**
     * Test update.
     */
    private void testUpdate()
    {
        Customer c = new Customer().find(101);
        c.setName("karthik");
        c.update();

        Customer c1 = new Customer().find(101);
        Assert.assertEquals(101, c1.getCustomerId());
        Assert.assertEquals("karthik", c1.getName());
        Assert.assertEquals("Noida", c1.getLocation());
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        Customer c = new Customer().find(101);
        c.delete();

        Customer c1 = new Customer().find(101);
        Assert.assertNull(c1);
    }

    /**
     * Tear down after class.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        Customer.unbind();
    }

}
