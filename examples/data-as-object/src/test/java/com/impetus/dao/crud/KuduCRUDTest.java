package com.impetus.dao.crud;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.dao.entities.Employee;

import junit.framework.Assert;

public class KuduCRUDTest
{
    @BeforeClass
    public static void SetUpBeforeClass()
    {
        Employee.bind("kudu-client.properties", Employee.class);
    }

    @Test
    public void testInsert()
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

    @Test
    public void testUpdate()
    {
        Employee e = new Employee().find(101l);
        e.setName("dev");
        e.update();

        Employee e1 = new Employee().find(101l);
        Assert.assertEquals(Long.valueOf(101), e1.getEmplyoeeId());
        Assert.assertEquals("dev", e1.getName());
        Assert.assertEquals(50000d, e1.getSalary());
    }

    @Test
    public void testDelete()
    {
        Employee e = new Employee().find(101l);
        e.delete();

        Employee e1 = new Employee().find(101l);
        Assert.assertNull(e1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        Employee.unbind();
    }

}
