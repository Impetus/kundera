package com.impetus.kundera.tests.crossdatastore.useraddress.entities;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.impetus.kundera.annotations.Index;

@Entity
@Index(index = true, columns = { "STREET" })
@Table(name = "ADDRESS", schema = "KunderaTests")
public class HabitatBiMTo1
{
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    @Column(name = "STREET")
    private String street;

    @OneToMany(mappedBy = "address", fetch = FetchType.LAZY)
    private Set<PersonnelBiMTo1> people;

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

    public Set<PersonnelBiMTo1> getPeople()
    {
        return people;
    }

    public void setPeople(Set<PersonnelBiMTo1> people)
    {
        this.people = people;
    }

}
