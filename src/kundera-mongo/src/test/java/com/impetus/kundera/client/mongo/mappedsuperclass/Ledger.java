package com.impetus.kundera.client.mongo.mappedsuperclass;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToOne;


@Entity
public class Ledger
{
    @Id
    private String ledgerId;
    
    @Column
    private Integer balance;

    @Column
    private String user;
    
    @OneToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    private Transaction trans;
    
    public Ledger()
    {
        
    }
    
    public String getLedgerId()
    {
        return ledgerId;
    }

    public void setLedgerId(String ledgerId)
    {
        this.ledgerId = ledgerId;
    }

    
    public Integer getBalanace()
    {
        return balance;
    }

    public void setBalanace(Integer balance)
    {
        this.balance = balance;
    }

    public String getPayee()
    {
        return user;
    }

    public void setPayee(String user)
    {
        this.user = user;
    }
    
    public Transaction getTransaction()
    {
        return trans;
    }

    public void setTransaction(Transaction trans)
    {
        this.trans = trans;
    }

}
