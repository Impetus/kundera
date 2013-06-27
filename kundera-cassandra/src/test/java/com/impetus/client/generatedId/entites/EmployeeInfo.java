package com.impetus.client.generatedId.entites;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

@Entity
@Table(name = "EmployeeInfo", schema = "kunderaGeneratedId@cassandra_generated_id")
public class EmployeeInfo
{
    @Id
    @Column(name = "UserID")
    @TableGenerator(name = "id_gen", allocationSize = 1, initialValue = 1)
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.TABLE)
    private Long userid;
    
    @Column(name="name")
    
    private String employeeName;
    
    public EmployeeInfo()
    {
        
    }
    
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "address_id")
    private EmployeeAddress address;

    public Long getUserid()
    {
        return userid;
    }

    public void setUserid(Long userid)
    {
        this.userid = userid;
    }

    public EmployeeAddress getAddress()
    {
        return address;
    }

    public void setAddress(EmployeeAddress address)
    {
        this.address = address;
    }

    public String getEmployeeName()
    {
        return employeeName;
    }

    public void setEmployeeName(String employeeName)
    {
        this.employeeName = employeeName;
    }
    
    
}
