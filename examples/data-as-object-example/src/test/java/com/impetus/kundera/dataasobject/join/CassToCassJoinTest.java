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
package com.impetus.kundera.dataasobject.join;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.dataasobject.entities.Department;
import com.impetus.kundera.dataasobject.entities.Employee;

import junit.framework.Assert;

/**
 * The Class CassToCassJoinTest.
 */
public class CassToCassJoinTest
{

    /**
     * Sets the up before class.
     */
    @BeforeClass
    public static void SetUpBeforeClass()
    {
        Employee.bind("client-properties.json", Employee.class);
        Department.bind("client-properties.json", Department.class);
    }

    /**
     * Sets the up.
     */
    @Before
    public void SetUp()
    {
        for (long i = 1; i <= 50; i++)
        {
            Employee empl = new Employee();
            empl.setEmplyoeeId(i);
            empl.setName("kart_" + i);
            empl.setSalary(i + 1000d);
            empl.save();
        }

        for (long i = 1; i <= 500; i++)
        {
            Department dept = new Department();
            dept.setDeptId(i + 100);
            dept.setEmployeeId(i + 10);
            dept.setDepartmentName("Department_" + i);
            dept.save();
        }
    }

    /**
     * Test join.
     */
    @Test
    public void testJoin()
    {
        Employee e = new Employee();
        List result = e.leftJoin(Department.class, "employeeId");
        Assert.assertEquals(40, result.size());
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        for (long i = 1; i <= 50; i++)
        {
            Employee e = new Employee().find(i);
            e.delete();
        }
        for (long i = 1; i <= 500; i++)
        {
            Department d = new Department().find(i + 100);
            d.delete();
        }
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
        Department.unbind();
    }

}
