package com.impetus.kundera.metadata.mappedsuperclass;

import java.math.BigInteger;

import javax.persistence.Column;
import javax.persistence.Entity;

@Entity
public class Employee extends MappedPerson
{

    @Column
    private BigInteger salary;
    
    @Column
    private Integer departmentId;

    public Employee()
    {
        
    }
    
    public BigInteger getSalary()
    {
        return salary;
    }

    public void setSalary(BigInteger salary)
    {
        this.salary = salary;
    }

    public Integer getDepartmentId()
    {
        return departmentId;
    }

    public void setDepartmentId(Integer departmentId)
    {
        this.departmentId = departmentId;
    }
    
    
}
