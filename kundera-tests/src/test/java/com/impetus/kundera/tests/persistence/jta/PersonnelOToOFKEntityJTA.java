package com.impetus.kundera.tests.persistence.jta;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@Table(name = "PERSONNEL", schema = "KunderaTests@secIdxAddCassandraJTA")
public class PersonnelOToOFKEntityJTA
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "ADDRESS_ID")
    private HabitatOToOFKEntityJTA address;

    
    /**
     * 
     */
    public PersonnelOToOFKEntityJTA()
    {
    }

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

    public HabitatOToOFKEntityJTA getAddress()
    {
        return address;
    }

    public void setAddress(HabitatOToOFKEntityJTA address)
    {
        this.address = address;
    }

}
