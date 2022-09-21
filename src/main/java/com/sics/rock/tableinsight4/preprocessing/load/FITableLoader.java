package com.sics.rock.tableinsight4.preprocessing.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * table loader interface
 *
 * @author zhaorx
 */
public interface FITableLoader {

    Dataset<Row> load(String tablePath);

}
