package com.impetus.client.twitter.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 *
 */
@Entity
@Table( name="User", schema = "KunderaExamples@mongoTest")
public class User
{
    @Id
    @Column
    private Integer userId;
    @Column
    private String name;
    @Column
    private String email;
    @Column
    private Integer age;
    @Column
    private String lastName;   
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name="rolId")
    private RoleMongo userRol;
  
    
    /**
     * 
     */
    public User()
    {
    }
    /**
     * @return the userId
     */
    public Integer getUserId()
    {
        return userId;
    }
    /**
     * @param userId the userId to set
     */
    public void setUserId(Integer userId)
    {
        this.userId = userId;
    }
    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }
    /**
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }
    /**
     * @param email the email to set
     */
    public void setEmail(String email)
    {
        this.email = email;
    }
    /**
     * @return the age
     */
    public Integer getAge()
    {
        return age;
    }
    /**
     * @param age the age to set
     */
    public void setAge(Integer age)
    {
        this.age = age;
    }
    /**
     * @return the lastName
     */
    public String getLastName()
    {
        return lastName;
    }
    /**
     * @param lastName the lastName to set
     */
    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }
    /**
     * @return the userRol
     */
    public RoleMongo getUserRol()
    {
        return userRol;
    }
    /**
     * @param userRol the userRol to set
     */
    public void setUserRol(RoleMongo userRol)
    {
        this.userRol = userRol;
    }
    
    
}
