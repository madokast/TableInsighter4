package com.sics.rock.tableinsight4.procedure.load;

import com.sics.rock.tableinsight4.test.FTiTestEnv;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

@Ignore
public class FPostgresJdbcTableLoaderTest extends FTiTestEnv {

    @Test
    public void test_jdbc() {
        Map<String, String> options = config().jdbcTableLoadOptions;
        options.put("url", "jdbc:postgresql://192.168.3.63:5432/testdata");
        options.put("user", "postgres");
        options.put("password", "postgres");
        FTableLoader loader = new FTableLoader();
        loader.load("public.relation_1").show();
    }
}