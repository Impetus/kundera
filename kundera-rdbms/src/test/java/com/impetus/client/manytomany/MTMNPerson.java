package com.impetus.client.manytomany;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "PERSON", schema = "test")
public class MTMNPerson
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    // @OneToMany(cascade=CascadeType.ALL)
    // @JoinColumn(name="PERSON_ID")
    // private Set<Address> addresses;

    @ManyToOne
    @JoinColumn(name = "ADDRESS_ID")
    private MTMAddress address;

    public String getPersonId()
    {
        return personId;
    }

    public String getPersonName()
    {
        return personName;
    }

    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    // public Set<Address> getAddresses() {
    // return addresses;
    // }
    //
    //
    // public void setAddresses(Set<Address> addresses) {
    // this.addresses = addresses;
    // }

    public MTMAddress getAddress()
    {
        return address;
    }

    /**
     * @param address
     *            the address to set
     */
    public void setAddress(MTMAddress address)
    {
        this.address = address;
    }

}
