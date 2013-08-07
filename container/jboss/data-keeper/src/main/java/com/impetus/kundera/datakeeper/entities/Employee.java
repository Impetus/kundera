package com.impetus.kundera.datakeeper.entities;

import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Kuldeep.Mishra
 * 
 */
@Entity
@Table(name = "EMPLOYEE", schema = "datakeeper@cassandra-pu")
@IndexCollection(columns = { @Index(name = "employeeName"), @Index(name = "designation"), @Index(name = "experience"),
        @Index(name = "joiningDate"), @Index(name = "currentProject"), @Index(name = "timestamp"),
        @Index(name = "company") })
public class Employee
{
    @Id
    @Column(name = "EMPLOYEE_ID")
    @GeneratedValue(strategy = GenerationType.TABLE)
    private int employeeId;

    @Column(name = "PASSWORD")
    private String password;

    @Column(name = "EMPLOYEE_NAME")
    private String employeeName;

    @Column(name = "EXPERIENCE")
    private int experience;

    @Column(name = "JOINING_DATE")
    private Date joiningDate;

    @Column(name = "DESIGNATION")
    private String designation;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "MANAGER_ID")
    private Employee manager;

    @OneToMany(fetch = FetchType.EAGER, mappedBy = "manager")
    private List<Employee> subordinates;

    @Column(name = "PROJECT")
    private String currentProject;

    @Column(name = "COMPANY")
    private String company;

    @Column(name = "JOINING_TIMESTAMP")
    private long timestamp;

    public int getEmployeeId()
    {
        return employeeId;
    }

    public void setEmployeeId(int employeeId)
    {
        this.employeeId = employeeId;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getEmployeeName()
    {
        return employeeName;
    }

    public void setEmployeeName(String employeeName)
    {
        this.employeeName = employeeName;
    }

    public int getExperience()
    {
        return experience;
    }

    public void setExperience(int experience)
    {
        this.experience = experience;
    }

    public Date getJoiningDate()
    {
        return joiningDate;
    }

    public void setJoiningDate(Date joiningDate)
    {
        this.joiningDate = joiningDate;
    }

    public String getDesignation()
    {
        return designation;
    }

    public void setDesignation(String designation)
    {
        this.designation = designation;
    }

    public Employee getManager()
    {
        return manager;
    }

    public void setManager(Employee manager)
    {
        this.manager = manager;
    }

    public String getCurrentProject()
    {
        return currentProject;
    }

    public void setCurrentProject(String currentProject)
    {
        this.currentProject = currentProject;
    }

    public List<Employee> getSubordinates()
    {
        return subordinates;
    }

    public void setSubordinates(List<Employee> subordinates)
    {
        this.subordinates = subordinates;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getCompany()
    {
        return company;
    }

    public void setCompany(String company)
    {
        this.company = company;
    }
}
