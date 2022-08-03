package com.sics.rock.tableinsight4.preprocessing.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaorx
 */
public interface FITableLoader {

    Dataset<Row> load(String tablePath);

}
