package com.impetus.client.oraclenosql.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "ADDRESS_OTO", schema = "KunderaTests@twikvstore")
@IndexCollection(columns = { @Index(name = "street") })
public class AddressOTOOracleNoSQL
{

    @Id
    @Column(name = "ADDRESS_ID")
    private Double addressId;

    @Column(name = "street")
    private String street;

    public AddressOTOOracleNoSQL()
    {

    }

    public AddressOTOOracleNoSQL(Double addressId, String address)
    {
        this.addressId = addressId;
        this.street = address;
    }

    public AddressOTOOracleNoSQL(Double addressId)
    {
        this.addressId = addressId;
    }

    /**
     * @return the street
     */
    public String getStreet()
    {
        return street;
    }

    /**
     * @param street
     *            the street to set
     */
    public void setStreet(String street)
    {
        this.street = street;
    }

    /**
     * @param addressId
     *            the addressId to set
     */
    public void setAddressId(Double addressId)
    {
        this.addressId = addressId;
    }

    public Double getAddressId()
    {
        return addressId;
    }

}
