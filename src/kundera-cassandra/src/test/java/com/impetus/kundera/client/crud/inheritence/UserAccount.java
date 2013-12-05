package com.impetus.kundera.client.crud.inheritence;


import java.util.List;
import java.util.Set;


import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.persistence.Table;


@Entity
@Table(name = "user_account")
public class UserAccount extends GuidDomainObject
{
    /*@EmbeddedId
    private ProfileType key;
    
    public ProfileType getId()
    {
        return key;
    }

    public void setId(ProfileType key)
    {
        this.key = key;
    }
    
    */
    @Column(name = "display_name", length = 128)
    private String displayName = null;

    @Column(name = "email", length = 128, unique = true)
    private String email = null;

    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER,mappedBy = "account")
    private List<SocialProfile> profiles;
    
    public String getDispName()
    {
        return displayName;
    }

    public void setDispName(String displayName)
    {
        this.displayName = displayName;
    }
    
    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }
    
    public List<SocialProfile> getSocialProfiles()
    {
        return profiles;
    }

    public void setSocialProfiles(List<SocialProfile> profiles)
    {
        this.profiles = profiles;
    }
} 
