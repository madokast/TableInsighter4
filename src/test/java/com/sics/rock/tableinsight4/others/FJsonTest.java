package com.sics.rock.tableinsight4.others;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.junit.Test;

import java.util.Map;

public class FJsonTest extends FBasicTestEnv {

    @Test
    public void test() throws JsonProcessingException {
        class Person {
            String name;
            Map<String, Object> others;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public Map<String, Object> getOthers() {
                return others;
            }

            public void setOthers(Map<String, Object> others) {
                this.others = others;
            }
        }
        final Person p = new Person();
        p.name = "a";
        p.others = FTiUtils.mapOf("age", 30);

        final ObjectMapper mapper = new ObjectMapper();
        final String ps = mapper.writeValueAsString(p);
        logger.info(ps);
    }

}
