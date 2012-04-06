package com.impetus.client.hbase.schemaManager;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

@Entity
@Table(name = "HbaseEntityPersonUni1To1PK", schema = "KunderaHbaseExamples@hbase")
public class HbaseEntityPersonUni1To1PK
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @Embedded
    HbasePersonalData personalData;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY, optional = false)
    @PrimaryKeyJoinColumn
    private HbaseEntityAddressUni1To1PK address;

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
     * @return the address
     */
    public HbaseEntityAddressUni1To1PK getAddress()
    {
        return address;
    }

    /**
     * @param address
     *            the address to set
     */
    public void setAddress(HbaseEntityAddressUni1To1PK address)
    {
        this.address = address;
    }
}
