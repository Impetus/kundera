package com.impetus.kundera.tests.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "CommonUser")
public class CommonUser
{

    /** The person id. */
    @Id
    private String userId;

    /** The first name. */
    @Column(name = "first_name")
    private String firstName;

    /** The last name. */
    @Column(name = "last_name")
    private String lastName;

    /**
     * Instantiates a new personnel dto.
     * 
     * @param userId
     *            the person id
     * @param firstName
     *            the first name
     * @param lastName
     *            the last name
     */
    public CommonUser(String userId, String firstName, String lastName)
    {
        this.userId = userId;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * Instantiates a new personnel dto.
     */
    public CommonUser()
    {

    }

    public String getUserId()
    {
        return userId;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    /**
     * Gets the first name.
     * 
     * @return the firstName
     */
    public String getFirstName()
    {
        return firstName;
    }

    /**
     * Sets the first name.
     * 
     * @param firstName
     *            the firstName to set
     */
    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * Gets the last name.
     * 
     * @return the lastName
     */
    public String getLastName()
    {
        return lastName;
    }

    /**
     * Sets the last name.
     * 
     * @param lastName
     *            the lastName to set
     */
    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }

}
