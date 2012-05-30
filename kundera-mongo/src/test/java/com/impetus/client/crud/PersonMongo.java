package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

/**
 * The Class Person.
 */
@Entity
@Table(name = "PERSON", schema = "KunderaExamples@mongoTest")
@NamedQueries(value={@NamedQuery(name="mongo.named.query", query="Select p from PersonMongo p where p.personName = :name"),@NamedQuery(name="mongo.position.query", query="Select p from PersonMongo p where p.personName = ?1")})

public class PersonMongo
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
    private Integer age;

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
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * @param age the age to set
     */
    public void setAge(int age)
    {
        this.age = age;
    }
    
    
}
