package com.impetus.kundera.tests.crossdatastore.useraddress.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@Table(name = "ADDRESS", schema = "KunderaTests")
public class HabitatBi1To1FK
{
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    @Column(name = "STREET")
    private String street;

    @OneToOne(mappedBy = "address")
    private PersonnelBi1To1FK person;

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

    public PersonnelBi1To1FK getPerson()
    {
        return person;
    }

    public void setPerson(PersonnelBi1To1FK person)
    {
        this.person = person;
    }

}
