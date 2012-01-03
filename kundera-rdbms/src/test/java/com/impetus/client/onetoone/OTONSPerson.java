package com.impetus.client.onetoone;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

@Entity
@Table(name = "PERSON", schema = "test")
public class OTONSPerson
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @OneToOne
    @PrimaryKeyJoinColumn
    private OTOSAddress address;

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

    public OTOSAddress getAddress()
    {
        return address;
    }

    /**
     * @param address
     *            the address to set
     */
    public void setAddress(OTOSAddress address)
    {
        this.address = address;
    }

}
