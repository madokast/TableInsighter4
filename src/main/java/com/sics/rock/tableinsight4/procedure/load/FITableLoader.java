package com.sics.rock.tableinsight4.procedure.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface FITableLoader {

    Dataset<Row> load(String tablePath);

}