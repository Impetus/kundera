package com.impetus.client.schemaManager;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * The Class CassandraEntityPersonUniMto1.
 */
@Entity
@Table(name = "CassandraEntityPersonUniMto1", schema = "KunderaCoreExmples@cassandra")
public class CassandraEntityPersonUniMto1
{

    /** The person id. */
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    /** The age. */
    @Column(name = "AGE")
    private short age;

    /** The personal data. */
    @Embedded
    private CassandraPersonalData personalData;

    /** The address. */
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "ADDRESS_ID")
    private CassandraEntityAddressUniMTo1 address;

    /**
     * Gets the person id.
     *
     * @return the person id
     */
    public String getPersonId()
    {
        return personId;
    }

    /**
     * Gets the person name.
     *
     * @return the person name
     */
    public String getPersonName()
    {
        return personName;
    }

    /**
     * Sets the person name.
     *
     * @param personName the new person name
     */
    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

    /**
     * Sets the person id.
     *
     * @param personId the new person id
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * Gets the age.
     *
     * @return the age
     */
    public short getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     *
     * @param age the age to set
     */
    public void setAge(short age)
    {
        this.age = age;
    }

    /**
     * Gets the personal data.
     *
     * @return the personalData
     */
    public CassandraPersonalData getPersonalData()
    {
        return personalData;
    }

    /**
     * Sets the personal data.
     *
     * @param personalData the personalData to set
     */
    public void setPersonalData(CassandraPersonalData personalData)
    {
        this.personalData = personalData;
    }

    /**
     * Gets the address.
     *
     * @return the address
     */
    public CassandraEntityAddressUniMTo1 getAddress()
    {
        return address;
    }

    /**
     * Sets the address.
     *
     * @param address the address to set
     */
    public void setAddress(CassandraEntityAddressUniMTo1 address)
    {
        this.address = address;
    }

}
