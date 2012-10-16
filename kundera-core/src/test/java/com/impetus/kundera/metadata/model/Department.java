/**
 * 
 */
package com.impetus.kundera.metadata.model;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Kuldeep Mishra
 * 
 */
@Embeddable
@IndexCollection(columns = { @com.impetus.kundera.index.Index(name = "email", type = "ASC"),
        @com.impetus.kundera.index.Index(name = "location", type = "GEO2D", min = -200, max = 200) })
public class Department
{

    /** The website. */
    @Column(name = "d_website")
    private String website;

    /** The email. */
    @Column(name = "d_email")
    private String email;

    /** The yahoo id. */
    @Column(name = "location")
    private Point location;

    /**
     * Instantiates a new core personal data.
     */
    public Department()
    {

    }

    /**
     * Instantiates a new core personal data.
     * 
     * @param website
     *            the website
     * @param email
     *            the email
     * @param yahooId
     *            the yahoo id
     */
    public Department(String website, String email, Point location)
    {
        this.website = website;
        this.email = email;
        this.location = location;
    }

    /**
     * Gets the website.
     * 
     * @return the website
     */
    public String getWebsite()
    {
        return website;
    }

    /**
     * Sets the website.
     * 
     * @param website
     *            the new website
     */
    public void setWebsite(String website)
    {
        this.website = website;
    }

    /**
     * Gets the email.
     * 
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

    /**
     * Sets the email.
     * 
     * @param email
     *            the email to set
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /**
     * Gets the yahoo id.
     * 
     * @return the yahoo id
     */
    public Point getLocation()
    {
        return location;
    }

    /**
     * Sets the yahoo id.
     * 
     * @param yahooId
     *            the new yahoo id
     */
    public void setLocation(Point location)
    {
        this.location = location;
    }

}
