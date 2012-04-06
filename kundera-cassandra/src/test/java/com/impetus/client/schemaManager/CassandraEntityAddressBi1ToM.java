package com.impetus.client.schemaManager;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * The Class CassandraEntityAddressBi1ToM.
 */
@Entity
@Table(name = "CassandraEntityAddressBi1ToM", schema = "KunderaCoreExmples@cassandra")
public class CassandraEntityAddressBi1ToM
{
    
    /** The address id. */
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    /** The street. */
    @Column(name = "STREET")
    private String street;

    /** The person. */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "PERSON_ID")
    private CassandraEntityPersonBi1ToM person;

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

    /**
     * Gets the person.
     *
     * @return the person
     */
    public CassandraEntityPersonBi1ToM getPerson()
    {
        return person;
    }

    /**
     * Sets the person.
     *
     * @param person the new person
     */
    public void setPerson(CassandraEntityPersonBi1ToM person)
    {
        this.person = person;
    }

}
