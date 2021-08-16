package org.simple.store.service;

import org.junit.Test;
import org.simple.store.ParentName;

import static org.junit.Assert.*;

public class UtilTest {

    @Test
    public void separateStore(){
        ParentName pn= EndPointAPI.getStoreKey("/maven2/");
        assertEquals("maven2", pn.parent);
    }

    @Test
    public void actionByName(){
        Action a= Action.valueOf("move");
        assertEquals(a, Action.move);
        a= Action.valueOf("copy");
        assertEquals(a, Action.copy);
    }
}
