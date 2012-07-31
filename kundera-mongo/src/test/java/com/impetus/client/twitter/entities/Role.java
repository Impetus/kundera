package com.impetus.client.twitter.entities;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 *
 */
@Entity
@Table( name="Rol", schema = "KunderaExamples@mongoTest")
public class Role
{

    @Id
    @Column
    private Integer rolId;
    @Column
    private String name;
    @OneToMany (cascade={CascadeType.ALL}, fetch=FetchType.LAZY, mappedBy="userRol")
    private List<User> segUsuarioList;


    public Role()
    {
        
    }
    
    /**
     * @return the rolId
     */
    public Integer getRolId()
    {
        return rolId;
    }
    /**
     * @param rolId the rolId to set
     */
    public void setRolId(Integer rolId)
    {
        this.rolId = rolId;
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
     * @return the segUsuarioList
     */
    public List<User> getSegUsuarioList()
    {
        return segUsuarioList;
    }
    /**
     * @param segUsuarioList the segUsuarioList to set
     */
    public void setSegUsuarioList(List<User> segUsuarioList)
    {
        this.segUsuarioList = segUsuarioList;
    }
    
    
}
