package com.impetus.client.schemaManager;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class CassandraEntitySimple.
 */
@Entity
@Table(name = "CassandraEntitySimple", schema = "KunderaCoreExmples@cassandra")
public class CassandraEntitySimple
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

}
