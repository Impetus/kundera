package com.impetus.client.hbase.schemaManager;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name = "HbaseEntityPersonUni1ToM", schema = "KunderaHbaseExamples@hbase")
public class HbaseEntityPersonUni1ToM
{

    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @Column(name = "AGE")
    private short age;

    @Embedded
    private HbasePersonalData personalData;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "PERSON_ID")
    private Set<HbaseEntityAddressUni1ToM> addresses;

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

    /**
     * @return the age
     */
    public short getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(short age)
    {
        this.age = age;
    }

    /**
     * @return the personalData
     */
    public HbasePersonalData getPersonalData()
    {
        return personalData;
    }

    /**
     * @param personalData
     *            the personalData to set
     */
    public void setPersonalData(HbasePersonalData personalData)
    {
        this.personalData = personalData;
    }

    /**
     * @return the addresses
     */
    public Set<HbaseEntityAddressUni1ToM> getAddresses()
    {
        return addresses;
    }

    /**
     * @param addresses
     *            the addresses to set
     */
    public void setAddresses(Set<HbaseEntityAddressUni1ToM> addresses)
    {
        this.addresses = addresses;
    }

}
