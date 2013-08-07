package com.impetus.kundera.datakeeper.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "SUBORDINATES_COUNTER", schema = "datakeeper@cassandra-pu")
public class SubordinatesCounter
{
    @Id
    @Column(name = "EMPLOYEE_ID")
    private int employeeId;

    @Column(name = "SUBORDINATES_COUNTER")
    private int noOfSubordinates;

    public int getEmployeeId()
    {
        return employeeId;
    }

    public void setEmployeeId(int employeeId)
    {
        this.employeeId = employeeId;
    }

    public int getNoOfSubordinates()
    {
        return noOfSubordinates;
    }

    public void setNoOfSubordinates(int noOfSubordinates)
    {
        this.noOfSubordinates = noOfSubordinates;
    }
}
