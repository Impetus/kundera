/**
 * 
 */
package com.impetus.kundera.tests.externalproeprties;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "ACCOUNTHOLDER", schema = "KunderaTests@secIdxAddCassandra")
//@IndexCollection(columns = { @Index(name = "PERSON_ID") })
public class AccountHolder
{

    @Id
    @Column(name = "ACCOUNTHOLDER_ID")
    private String accountHolderId;

    @Column(name = "ACCOUNTHOLDER_NAME")
    private String accountHoldername;

    @Column(name = "TOTALBALANCE")
    private String totalBalance;

    /**
     * @return the accountHolderId
     */
    public String getAccountHolderId()
    {
        return accountHolderId;
    }

    /**
     * @param accountHolderId
     *            the accountHolderId to set
     */
    public void setAccountHolderId(String accountHolderId)
    {
        this.accountHolderId = accountHolderId;
    }

    /**
     * @return the accountHoldername
     */
    public String getAccountHoldername()
    {
        return accountHoldername;
    }

    /**
     * @param accountHoldername
     *            the accountHoldername to set
     */
    public void setAccountHoldername(String accountHoldername)
    {
        this.accountHoldername = accountHoldername;
    }

    /**
     * @return the totalBalance
     */
    public String getTotalBalance()
    {
        return totalBalance;
    }

    /**
     * @param totalBalance
     *            the totalBalance to set
     */
    public void setTotalBalance(String totalBalance)
    {
        this.totalBalance = totalBalance;
    }
}
