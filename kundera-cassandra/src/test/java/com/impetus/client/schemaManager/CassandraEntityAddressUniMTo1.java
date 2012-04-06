package com.impetus.client.schemaManager;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class CassandraEntityAddressUniMTo1.
 */
@Entity
@Table(name = "CassandraEntityAddressUniMTo1", schema = "KunderaCoreExmples@cassandra")
public class CassandraEntityAddressUniMTo1
{
    
    /** The address id. */
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    /** The street. */
    @Column(name = "STREET")
    private String street;

    /**
     * Gets the address id.
     *
     * @return the address id
     */
    public String getAddressId()
    {
        return addressId;
    }

    /**
     * Sets the address id.
     *
     * @param addressId the new address id
     */
    public void setAddressId(String addressId)
    {
        this.addressId = addressId;
    }

    /**
     * Gets the street.
     *
     * @return the street
     */
    public String getStreet()
    {
        return street;
    }

    /**
     * Sets the street.
     *
     * @param street the new street
     */
    public void setStreet(String street)
    {
        this.street = street;
    }
}
