/**
 * 
 */
package com.impetus.kundera.tests.externalproeprties;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "BANK", schema = "KunderaTests@addMongo")
public class Bank
{

    @Id
    @Column(name = "BANK_ID")
    private String bankId;

    @Column(name = "BANK_NAME")
    private String bankName;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "PERSON_ID")
    private Set<AccountHolder> accountHolders;

    /**
     * @return the bankId
     */
    public String getBankId()
    {
        return bankId;
    }

    /**
     * @param bankId
     *            the bankId to set
     */
    public void setBankId(String bankId)
    {
        this.bankId = bankId;
    }

    /**
     * @return the bankName
     */
    public String getBankName()
    {
        return bankName;
    }

    /**
     * @param bankName
     *            the bankName to set
     */
    public void setBankName(String bankName)
    {
        this.bankName = bankName;
    }

    /**
     * @return the accountHolders
     */
    public Set<AccountHolder> getAccountHolders()
    {
        return accountHolders;
    }

    /**
     * @param accountHolders
     *            the accountHolders to set
     */
    public void setAccountHolders(Set<AccountHolder> accountHolders)
    {
        this.accountHolders = accountHolders;
    }
}
