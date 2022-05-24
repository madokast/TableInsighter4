package com.sics.rock.tableinsight4.test;

import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Example data
 */
public class FExamples {

    public FTableData relation() {
        String header = "cc,ac,pn,nm,str,ct,zip";
        Dataset<Row> data = FTableCreator.use(spark).create(
                header,
                ("01, 908, 1111111, Mike, Tree Ave, MH, 07974\n" +
                        "01, 908, 1111111, Rick, Tree Ave, MH, 07974\n" +
                        "01, 212, 2222222, Joe, 5th Ave, NYC, 01202\n" +
                        "01, 908, 2222222, Jim, Elm Str, MH, 07974\n" +
                        "01, 131, 2222222, Sean, 3rd Str, UN, 01202\n" +
                        "44, 131, 3333333, Ben, High St, EDI, EH4 1DT\n" +
                        "44, 131, 4444444, Ian, High St, EDI, EH4 1DT\n" +
                        "44, 908, 4444444, Ian, Port PI, MH, W1B 1JH").split("\n")
        );

        FTableInfo tableInfo = new FTableInfo("relation", "tab01");
        for (String colName : header.split(",")) {
            tableInfo.addColumnInfo(new FColumnInfo(colName, String.class));
        }

        return new FTableData(tableInfo, data);
    }


    private SparkSession spark;

    public static FExamples use(SparkSession spark) {
        FExamples e = new FExamples();
        e.spark = spark;
        return e;
    }
}
