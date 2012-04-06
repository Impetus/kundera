package com.impetus.client.schemaManager;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * The Class CassandraEntityPersonBi1ToM.
 */
@Entity
@Table(name = "CassandraEntityPersonBi1ToM", schema = "KunderaCoreExmples@cassandra")
public class CassandraEntityPersonBi1ToM
{
    
    /** The person id. */
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    /** The personal data. */
    @Embedded
    private CassandraPersonalData personalData;

    /** The addresses. */
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "person")
    private Set<CassandraEntityAddressBi1ToM> addresses;

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
     * Sets the personal data.
     *
     * @param personalData the personalData to set
     */
    public void setPersonalData(CassandraPersonalData personalData)
    {
        this.personalData = personalData;
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
     * Gets the addresses.
     *
     * @return the addresses
     */
    public Set<CassandraEntityAddressBi1ToM> getAddresses()
    {
        return addresses;
    }

    /**
     * Sets the addresses.
     *
     * @param addresses the new addresses
     */
    public void setAddresses(Set<CassandraEntityAddressBi1ToM> addresses)
    {
        this.addresses = addresses;
    }
}
