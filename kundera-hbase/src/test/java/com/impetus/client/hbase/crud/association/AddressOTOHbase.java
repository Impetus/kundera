package com.impetus.client.hbase.crud.association;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name="Address", schema="KunderaExamples@hbaseTest")
public class AddressOTOHbase
{

    @Id
    @Column(name="ADDRESS_ID")
    private Double addressId;
    
    public AddressOTOHbase()
    {
        
    }
    public AddressOTOHbase(Double addressId)
    {
        this.addressId = addressId;
    }
    
    @Column(name="address")
    private String address;

    public String getAddress()
    {
        return address;
    }

    public void setAddress(String address)
    {
        this.address = address;
    }

    
    public Double getAddressId()
    {
        return addressId;
    }
    
    
}
