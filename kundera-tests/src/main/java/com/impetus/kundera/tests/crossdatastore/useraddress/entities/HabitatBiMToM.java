package com.impetus.kundera.tests.crossdatastore.useraddress.entities;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

@Entity
@Table(name = "ADDRESS", schema = "KunderaTests")
public class HabitatBiMToM
{
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    @Column(name = "STREET")
    private String street;

    @ManyToMany(mappedBy = "addresses", fetch = FetchType.LAZY)
    private Set<PersonnelBiMToM> people;

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

    public Set<PersonnelBiMToM> getPeople()
    {
        return people;
    }

    public void setPeople(Set<PersonnelBiMToM> people)
    {
        this.people = people;
    }

}
