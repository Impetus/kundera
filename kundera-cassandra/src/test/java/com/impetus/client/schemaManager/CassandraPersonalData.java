package com.impetus.client.schemaManager;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class CassandraPersonalData
{
    @Column(name = "p_website")
    private String website;

    @Column(name = "p_email")
    private String email;

    @Column(name = "p_yahoo_id")
    private String yahooId;

    public CassandraPersonalData()
    {

    }

    public CassandraPersonalData(String website, String email, String yahooId)
    {
        this.website = website;
        this.email = email;
        this.yahooId = yahooId;
    }

    public String getWebsite()
    {
        return website;
    }

    public void setWebsite(String website)
    {
        this.website = website;
    }

    /**
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

    /**
     * @param email
     *            the email to set
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    public String getYahooId()
    {
        return yahooId;
    }

    public void setYahooId(String yahooId)
    {
        this.yahooId = yahooId;
    }
}
