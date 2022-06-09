package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Test;

import static org.junit.Assert.*;

public class FValueDefaultTest extends FBasicTestEnv {

    @Test
    public void getOrDefault() {
        assertEquals(10, FValueDefault.getOrDefault((Integer) 10, 11));
        assertEquals(11, FValueDefault.getOrDefault((Integer) null, 11));
    }

    @Test
    public void getOrDefault2() {
        assertEquals(10D, FValueDefault.getOrDefault((Double) 10D, 11D), 0);
        assertEquals(11D, FValueDefault.getOrDefault((Double) null, 11D), 0);
    }

    @Test
    public void getOrDefault3() {
        assertEquals("a", FValueDefault.getOrDefault("a", "b"));
        assertEquals("b", FValueDefault.getOrDefault(null, "b"));
    }
}