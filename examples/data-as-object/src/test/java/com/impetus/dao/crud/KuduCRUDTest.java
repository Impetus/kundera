package com.impetus.dao.crud;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.dao.entities.Customer;

import junit.framework.Assert;

public class KuduCRUDTest
{
    @BeforeClass
    public static void SetUpBeforeClass()
    {
        Customer.bind("client-properties.json", Customer.class);
    }

    @Test
    public void testCRUD()
    {
        testInsert();
        testUpdate();
        testDelete();
    }

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

    private void testDelete()
    {
        Customer c = new Customer().find(101);
        c.delete();

        Customer c1 = new Customer().find(101);
        Assert.assertNull(c1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        Customer.unbind();
    }

}
