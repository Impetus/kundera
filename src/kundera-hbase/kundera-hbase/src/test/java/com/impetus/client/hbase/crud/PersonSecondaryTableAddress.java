package com.impetus.client.hbase.crud;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;


@Entity
public class PersonSecondaryTableAddress
{
    @Id
    @Column(name = "ADDRESS_ID")
    private Double addressId;

    public PersonSecondaryTableAddress()
    {

    }

    public PersonSecondaryTableAddress(Double addressId)
    {
        this.addressId = addressId;
    }

    @Column(name = "address")
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
