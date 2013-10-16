package com.impetus.client.couchdb.entities;

import java.util.UUID;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.annotate.JsonProperty;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class CouchUser
{
    private String _id;

    private int age;

    private String name;

    public String getId()
    {
        return _id;
    }

    public void setId(String id)
    {
        this._id = id;
    }

    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public static void main(String[] args)
    {
        String id = UUID.randomUUID().toString();
        put(id);
        get(id);
    }

    private static void put(String id)
    {
        String input = "{ \"id\" : \"" + id + "\" , \"userName\" : \"Amresh\", \"age\" : 32, \"address\" : \"Noida\" }";
        try
        {
            Client client = Client.create();

            WebResource webResource = client.resource("http://localhost:5984/mydatabase/");

            ClientResponse response = webResource.accept(MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN,
                    MediaType.APPLICATION_JSON).post(ClientResponse.class, input);

            if (response.getStatus() != 200)
            {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }

            String output = response.getEntity(String.class);

            System.out.println(response.getLanguage());

            System.out.println(response.getType());

            System.out.println(response.getClient());

            System.out.println("Output from Server .... \n");

            System.out.println(output);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void get(String id)
    {
        try
        {
            Client client = Client.create();

            WebResource webResource = client.resource("http://localhost:5984/mydatabase/" + id);

            ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

            if (response.getStatus() != 200)
            {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }

            String output = response.getEntity(String.class);

            System.out.println(response.getLanguage());

            System.out.println(response.getType());

            System.out.println(response.getClient());

            System.out.println("Output from Server .... \n");

            System.out.println(output);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}