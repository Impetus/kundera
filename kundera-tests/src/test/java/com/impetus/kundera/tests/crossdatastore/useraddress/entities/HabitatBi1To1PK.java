package com.impetus.kundera.tests.crossdatastore.useraddress.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@Table(name = "ADDRESS", schema = "hibernatepoc")
public class HabitatBi1To1PK
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "ADDRESS_ID")
    private String addressId;

    @Column(name = "STREET")
    private String street;

    @OneToOne(mappedBy = "address", fetch = FetchType.LAZY)
    private PersonnelBi1To1PK person;

    public String getPersonId()
    {
        return personId;
    }

    public void setPersonId(String personId)
    {
        this.personId = personId;
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

    public PersonnelBi1To1PK getPerson()
    {
        return person;
    }

    public void setPerson(PersonnelBi1To1PK person)
    {
        this.person = person;
    }

}