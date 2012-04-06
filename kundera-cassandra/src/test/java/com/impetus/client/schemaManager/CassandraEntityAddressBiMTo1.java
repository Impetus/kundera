package com.impetus.client.schemaManager;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * The Class CassandraEntityAddressBiMTo1.
 */
@Entity
@Table(name = "CassandraEntityAddressBiMTo1", schema = "KunderaCoreExmples@cassandra")
public class CassandraEntityAddressBiMTo1
{
    
    /** The address id. */
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    /** The street. */
    @Column(name = "STREET")
    private String street;

    /** The people. */
    @OneToMany(mappedBy = "address", fetch = FetchType.LAZY)
    private Set<CassandraEntityPersonBiMTo1> people;

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
     * Gets the people.
     *
     * @return the people
     */
    public Set<CassandraEntityPersonBiMTo1> getPeople()
    {
        return people;
    }

    /**
     * Sets the people.
     *
     * @param people the new people
     */
    public void setPeople(Set<CassandraEntityPersonBiMTo1> people)
    {
        this.people = people;
    }

}
