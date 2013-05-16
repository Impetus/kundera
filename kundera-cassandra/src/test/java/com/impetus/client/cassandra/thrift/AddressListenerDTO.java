package com.impetus.client.cassandra.thrift;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "address", schema = "KunderaExamples@secIdxCassandraTest")
@EntityListeners({ AddressHandler.class })
public class AddressListenerDTO
{
    @Id
    private String addressId;

    @Column
    private String street;

    public String getAddressId()
    {
        return addressId;
    }

    public void setAddressId(String addressId)
    {
        this.addressId = addressId;
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
