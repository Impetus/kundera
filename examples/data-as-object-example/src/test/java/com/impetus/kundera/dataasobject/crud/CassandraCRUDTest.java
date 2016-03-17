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

import com.impetus.kundera.dataasobject.entities.Employee;

import junit.framework.Assert;

/**
 * The Class CassandraCRUDTest.
 */
public class CassandraCRUDTest
{

    /**
     * Sets the up before class.
     */
    @BeforeClass
    public static void SetUpBeforeClass()
    {
        Employee.bind("client-properties.json", Employee.class);
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
        Employee emp = new Employee();
        emp.setEmplyoeeId(101l);
        emp.setName("karthik");
        emp.setSalary(50000d);
        emp.save();
        Employee e = new Employee().find(101l);
        Assert.assertEquals(Long.valueOf(101), e.getEmplyoeeId());
        Assert.assertEquals("karthik", e.getName());
        Assert.assertEquals(50000d, e.getSalary());
    }

    /**
     * Test update.
     */
    private void testUpdate()
    {
        Employee e = new Employee().find(101l);
        e.setName("dev");
        e.update();

        Employee e1 = new Employee().find(101l);
        Assert.assertEquals(Long.valueOf(101), e1.getEmplyoeeId());
        Assert.assertEquals("dev", e1.getName());
        Assert.assertEquals(50000d, e1.getSalary());
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        Employee e = new Employee().find(101l);
        e.delete();

        Employee e1 = new Employee().find(101l);
        Assert.assertNull(e1);
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
        Employee.unbind();
    }
}
