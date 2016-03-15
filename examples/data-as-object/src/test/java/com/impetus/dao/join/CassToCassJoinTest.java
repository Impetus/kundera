package com.impetus.dao.join;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.dao.entities.Department;
import com.impetus.dao.entities.Employee;

import junit.framework.Assert;

public class CassToCassJoinTest
{

    @BeforeClass
    public static void SetUpBeforeClass()
    {
        Employee.bind("client-properties.json", Employee.class);
        Department.bind("client-properties.json", Department.class);
    }

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

    @Test
    public void testJoin()
    {
        Employee e = new Employee();
        List result = e.leftJoin(Department.class, "employeeId");
        Assert.assertEquals(40, result.size());
    }

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

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        Employee.unbind();
        Department.unbind();
    }

}
