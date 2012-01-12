package com.impetus.client.onetomany.bi;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name = "ADDRESS", schema = "KunderaKeyspace@kcassandra")
public class OTMBAddress
{
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    @Column(name = "STREET")
    private String street;

    @OneToMany(mappedBy = "address")
    // pointing Person's address field
    @Column(name = "PERSON_ID")
    // inverse=true
    private Set<OTMBNPerson> people;

    public OTMBAddress()
    {

    }

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

    /**
     * @return the people
     */
    public Set<OTMBNPerson> getPeople()
    {
        return people;
    }

    /**
     * @param people
     *            the people to set
     */
    public void setPeople(Set<OTMBNPerson> people)
    {
        this.people = people;
    }

}
