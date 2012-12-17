/**
 * 
 */
package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "TUSER", schema = "KunderaExamples@secIdxCassandraTest")
public class TransientUser
{

    @Id
    @Column(name = "USER_ID")
    private String userId;

    @Column(name = "USER_NAME")
    private String userName;

    @Column(name = "U_NAME")
    private transient String uName;

    public TransientUser()
    {
        // TODO Auto-generated constructor stub
    }

    /**
     * @return the personId
     */
    public String getPersonId()
    {
        return userId;
    }

    /**
     * @param personId
     *            the personId to set
     */
    public void setPersonId(String personId)
    {
        this.userId = personId;
    }

    /**
     * @return the personName
     */
    public String getPersonName()
    {
        return userName;
    }

    /**
     * @param personName
     *            the personName to set
     */
    public void setPersonName(String personName)
    {
        this.userName = personName;
    }

    /**
     * @return the uName
     */
    public String getuName()
    {
        return uName;
    }

    /**
     * @param uName
     *            the uName to set
     */
    public void setuName(String uName)
    {
        this.uName = uName;
    }

}
