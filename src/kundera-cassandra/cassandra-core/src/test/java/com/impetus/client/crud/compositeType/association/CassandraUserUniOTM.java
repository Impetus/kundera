package com.impetus.client.crud.compositeType.association;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name = "CassandraUserUniOTM", schema = "KunderaExamples@secIdxCassandraTest")
public class CassandraUserUniOTM
{

    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "PERSON_ID")
    private Set<CassandraAddressUniOTM> addresses;

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

    public Set<CassandraAddressUniOTM> getAddresses()
    {
        return addresses;
    }

    public void setAddresses(Set<CassandraAddressUniOTM> addresses)
    {
        this.addresses = addresses;
    }

}
