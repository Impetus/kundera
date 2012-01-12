/**
 * 
 */
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 * 
 */
@Entity
@Table(name = "snsusers", schema = "Blog@cassandra")
public class SnsUser
{
    @Id
    private String snsuid;

    @Column(name = "blessuid")
    private String blessuid;

    @Column(name = "snstype")
    private String snstype;

    @Column(name = "localId")
    private String localId;

    @Column(name = "accountName")
    private String accountName;

    /**
     * Default constructor with no fields.
     */
    public SnsUser()
    {
    }

    /**
     * @return the snsuid
     */
    public String getSnsuid()
    {
        return snsuid;
    }

    /**
     * @return the blessuid
     */
    public String getBlessuid()
    {
        return blessuid;
    }

    /**
     * @return the snstype
     */
    public String getSnstype()
    {
        return snstype;
    }

    /**
     * @return the localId
     */
    public String getLocalId()
    {
        return localId;
    }

    /**
     * @return the accountName
     */
    public String getAccountName()
    {
        return accountName;
    }

    /**
     * @param snsuid
     *            the snsuid to set
     */
    public void setSnsuid(String snsuid)
    {
        this.snsuid = snsuid;
    }

    /**
     * @param blessuid
     *            the blessuid to set
     */
    public void setBlessuid(String blessuid)
    {
        this.blessuid = blessuid;
    }

    /**
     * @param snstype
     *            the snstype to set
     */
    public void setSnstype(String snstype)
    {
        this.snstype = snstype;
    }

    /**
     * @param localId
     *            the localId to set
     */
    public void setLocalId(String localId)
    {
        this.localId = localId;
    }

    /**
     * @param accountName
     *            the accountName to set
     */
    public void setAccountName(String accountName)
    {
        this.accountName = accountName;
    }

}