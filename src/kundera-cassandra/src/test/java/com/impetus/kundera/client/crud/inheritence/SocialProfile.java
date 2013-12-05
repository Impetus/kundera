package com.impetus.kundera.client.crud.inheritence;

import java.io.Serializable;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.FetchType;
import javax.persistence.InheritanceType;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@Table(name = "social_profile")
@DiscriminatorColumn(name = "type", length = 64, discriminatorType = DiscriminatorType.STRING)
public abstract class SocialProfile extends GuidDomainObject implements Serializable
{
    @Column(name = "userType")
    private String userType;

    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "user_account_id")
    private UserAccount account;
    
    public String getuserType()
    {
        return userType;
    }

    public void setuserType(String userType)
    {
        this.userType = userType;
    }
    
    public UserAccount getuserAccount()
    {
        return account;
    }

    public void setuserAccount(UserAccount account)
    {
        this.account = account;
    }

}