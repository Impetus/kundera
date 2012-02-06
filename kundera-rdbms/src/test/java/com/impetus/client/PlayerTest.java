/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * The Class PlayerTest.
 */
public class PlayerTest
{

    /**
     * Test persist.
     */
    @Test
    public void testPersist()
    {
    }

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Prepare object.
     *
     * @return the player
     */
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