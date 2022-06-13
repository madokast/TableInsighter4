package com.sics.rock.tableinsight4.conf;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FTiConfiguringTest extends FBasicTestEnv {

    @Test
    public void test() {
        final FTiConfiguring object = new FTiConfiguring() {
        };

        object.config("abc.def", "123");
        object.config("abc.fun", "true");

        assertEquals(Integer.valueOf(123), object.getConfig("def", 12));
        assertEquals(Integer.valueOf(12), object.getConfig("other", 12));

        assertEquals(Boolean.TRUE, object.getConfig("fun", false));
        assertEquals(Boolean.FALSE, object.getConfig("other", false));
    }

    @Test(expected = Exception.class)
    public void test2() {
        if (logger.isDebugEnabled()) {
            final FTiConfiguring object = new FTiConfiguring() {
            };

            object.config("abc.def", "123");
            object.config("abc.fun", "321");

            final Integer abc = object.getConfig("abc", 333);

            logger.info("key abc {}", abc);

        } else {
            throw new RuntimeException("ignore test");
        }
    }

}