package com.impetus.client.generatedId.entites;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

@Entity
@Table(name = "EmployeeAddress", schema = "kunderaGeneratedId@cassandra_generated_id")
public class EmployeeAddress
{

    @Id
    @Column(name = "RegionID")
    @TableGenerator(name = "id_gen", allocationSize = 1, initialValue = 1)
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.TABLE)
    private Long address;
    
    @Column(name="street")
    private String street;

    public EmployeeAddress()
    {
        
    }
    public Long getAddress()
    {
        return address;
    }

    public void setAddress(Long address)
    {
        this.address = address;
    }

    public String getStreet()
    {
        return street;
    }

    public void setStreet(String street)
    {
        this.street = street;
    }
    
    
}
