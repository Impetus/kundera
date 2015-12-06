package com.impetus.client.hbase.query;

import static com.impetus.client.hbase.query.LikeComparatorFactory.likeToRegex;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LikeComparatorFactoryTest {
    @Test
    public void likeToRegexConversion() {
        assertEquals("as is", likeToRegex("as is"));
        assertEquals("last percent .*", likeToRegex("last percent %"));
        assertEquals("middle.*percent", likeToRegex("middle%percent"));
        assertEquals(".*first percent", likeToRegex("%first percent"));
        assertEquals("%", likeToRegex("%%"));
        assertEquals("%.*", likeToRegex("%%%"));
        assertEquals("\\" + "{" + "\\" + "[", likeToRegex("{["));
    }
}
