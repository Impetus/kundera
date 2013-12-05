package com.impetus.kundera.client.crud.inheritence;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
@DiscriminatorValue("fb")
public class FacebookProfile extends SocialProfile
{
    // protected static final String TYPE = "twitter";

    // @Id
    // // @GeneratedValue
    // @Column(name = "guid", updatable = false, nullable = false)
    // private String id;

    @Column(name = "facebook_id", updatable = false)
    private String facebookId;

    @Column(name = "twitter_user", length = 128)
    private String facebookUser;

    public String getFacebookId()
    {
        return facebookId;
    }

    public void setFacebookId(String facebookId)
    {
        this.facebookId = facebookId;
    }

    public String getFacebookUser()
    {
        return facebookUser;
    }

    public void setFacebookUser(String facebookUser)
    {
        this.facebookUser = facebookUser;
    }

}
