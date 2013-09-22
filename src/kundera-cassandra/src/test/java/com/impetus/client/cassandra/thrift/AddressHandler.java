package com.impetus.client.cassandra.thrift;

import javax.persistence.PostPersist;
import javax.persistence.PrePersist;

public class AddressHandler
{

    @PrePersist
    public void handledPrePersist(AddressListenerDTO address)
    {
        address.setStreet("aaaa");
    }

    @PostPersist
    public void handledPostPersist(AddressListenerDTO address)
    {
        address.setStreet("bbbb");
    }
}
