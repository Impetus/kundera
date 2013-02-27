package com.impetus.client.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name="Address", schema="RedisK@redis_pu")
public class AddressOTORedis
{

    @Id
    @Column(name="ADDRESS_ID")
    private Double addressId;
    
    public AddressOTORedis()
    {
        
    }
    public AddressOTORedis(Double addressId)
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
