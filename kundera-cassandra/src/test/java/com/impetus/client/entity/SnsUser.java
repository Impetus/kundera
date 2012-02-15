/**
 * 
 */
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class SnsUser.
 * 
 * @author vivek.mishra
 */
@Entity
@Table(name = "snsusers", schema = "Blog@cassandra")
public class SnsUser
{

    /** The snsuid. */
    @Id
    private String snsuid;

    /** The blessuid. */
    @Column(name = "blessuid")
    private String blessuid;

    /** The snstype. */
    @Column(name = "snstype")
    private String snstype;

    /** The local id. */
    @Column(name = "localId")
    private String localId;

    /** The account name. */
    @Column(name = "accountName")
    private String accountName;

    /**
     * Default constructor with no fields.
     */
    public SnsUser()
    {
    }

    /**
     * Gets the snsuid.
     * 
     * @return the snsuid
     */
    public String getSnsuid()
    {
        return snsuid;
    }

    /**
     * Gets the blessuid.
     * 
     * @return the blessuid
     */
    public String getBlessuid()
    {
        return blessuid;
    }

    /**
     * Gets the snstype.
     * 
     * @return the snstype
     */
    public String getSnstype()
    {
        return snstype;
    }

    /**
     * Gets the local id.
     * 
     * @return the localId
     */
    public String getLocalId()
    {
        return localId;
    }

    /**
     * Gets the account name.
     * 
     * @return the accountName
     */
    public String getAccountName()
    {
        return accountName;
    }

    /**
     * Sets the snsuid.
     * 
     * @param snsuid
     *            the snsuid to set
     */
    public void setSnsuid(String snsuid)
    {
        this.snsuid = snsuid;
    }

    /**
     * Sets the blessuid.
     * 
     * @param blessuid
     *            the blessuid to set
     */
    public void setBlessuid(String blessuid)
    {
        this.blessuid = blessuid;
    }

    /**
     * Sets the snstype.
     * 
     * @param snstype
     *            the snstype to set
     */
    public void setSnstype(String snstype)
    {
        this.snstype = snstype;
    }

    /**
     * Sets the local id.
     * 
     * @param localId
     *            the localId to set
     */
    public void setLocalId(String localId)
    {
        this.localId = localId;
    }

    /**
     * Sets the account name.
     * 
     * @param accountName
     *            the accountName to set
     */
    public void setAccountName(String accountName)
    {
        this.accountName = accountName;
    }

}