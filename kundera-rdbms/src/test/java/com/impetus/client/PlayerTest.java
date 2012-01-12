package com.impetus.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PlayerTest
{

    @Test
    public void testPersist()
    {
    }

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }

    private Player prepareObject()
    {
        Player player = new Player();
        player.setFirstName("vivek");
        player.setJerseyNumber(10);
        player.setLastName("mishra");
        player.setId("1");
        player.setLastSpokenWords("i will finish it to win!");
        return player;
    }
}