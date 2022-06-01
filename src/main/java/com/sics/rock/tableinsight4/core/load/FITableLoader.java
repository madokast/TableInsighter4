package com.sics.rock.tableinsight4.core.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaorx
 */
public interface FITableLoader {

    Dataset<Row> load(String tablePath);

}