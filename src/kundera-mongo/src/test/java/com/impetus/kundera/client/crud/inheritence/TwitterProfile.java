package com.impetus.kundera.client.crud.inheritence;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
@DiscriminatorValue("twitter")
public class TwitterProfile extends SocialProfile
{
   // protected static final String TYPE = "twitter";

//    @Id
////    @GeneratedValue
//    @Column(name = "guid", updatable = false, nullable = false)
//    private String id;

    @Column(name = "twitter_id", updatable = false)
    private String twitterId;

    @Column(name = "twitter_user", length = 128)
    private String twitterUser;
    
    public String getTwitterId()
    {
        return twitterId;
    }

    public void setTwitterId(String twitterId)
    {
        this.twitterId = twitterId;
    }
    
    public String getTwitterName()
    {
        return twitterUser;
    }

    public void setTwitterName(String twitterUser)
    {
        this.twitterUser = twitterUser;
    }


}  
