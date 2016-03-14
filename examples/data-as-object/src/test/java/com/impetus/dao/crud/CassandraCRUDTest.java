package com.impetus.dao.crud;

import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.dao.entities.Department;
import com.impetus.dao.entities.Employee;

public class CassandraCRUDTest
{

    @BeforeClass
    public static void SetUpBeforeClass()
    {
        Employee.bind("cass-client.properties", Employee.class);
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
    
    @Test
    public void testJoin()
    {
    	for (long i = 1; i <= 50; i++) {
			Employee empl = new Employee();
			empl.setEmplyoeeId(i);
			empl.setName("kart_" + i);
			empl.setSalary(i + 1000d);
			empl.save();
		}
    	for (long i = 1; i <= 50; i++) {
			Department dept = new Department();
			dept.setDeptId(i+100);
			dept.setEmployeeId((long) (Math.random()%50));
			dept.setDepartmentName("Department_"+i);
			dept.save();
		}
    	Employee e = new Employee();
    	List result = e.leftJoin(Department.class, "employeeId");
    	System.out.println(result);
    }

    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        Employee.unbind();
    }
}
