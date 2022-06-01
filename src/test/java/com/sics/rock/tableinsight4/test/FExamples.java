package com.sics.rock.tableinsight4.test;

import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FValueType;

import java.io.File;
import java.util.Random;

/**
 * Example data
 */
public class FExamples {

    private static long rowId = new Random().nextLong();

    public static FTableInfo relation() {
        return relation("relation", "tab001");
    }

    public static FTableInfo relation(String tabName, String innerTabName) {
        String header = "cc,ac,pn,nm,str,ct,zip,row_id";

        String[] contents = {"01, 908, 1111111, Mike, Tree Ave, MH, 07974," + (++rowId),
                "01, 908, 1111111, Rick, Tree Ave, MH, 07974," + (++rowId),
                "01, 212, 2222222, Joe, 5th Ave, NYC, 01202," + (++rowId),
                "01, 908, 2222222, Jim, Elm Str, MH, 07974," + (++rowId),
                "01, 131, 2222222, Sean, 3rd Str, UN, 01202," + (++rowId),
                "44, 131, 3333333, Ben, High St, EDI, EH4 1DT," + (++rowId),
                "44, 131, 4444444, Ian, High St, EDI, EH4 1DT," + (++rowId),
                "44, 908, 4444444, Ian, Port PI, MH, W1B 1JH," + (++rowId)};

        File tab = FTableCreator.createCsv(header, contents);

        FTableInfo tableInfo = new FTableInfo(tabName, innerTabName, tab.getAbsolutePath());
        for (String colName : header.split(",")) {
            FValueType type = FValueType.STRING;
            if (colName.equals("row_id")) type = FValueType.LONG;
            tableInfo.addColumnInfo(new FColumnInfo(colName, type));
        }

        return tableInfo;
    }


    public static FTableInfo doubleNumberNullColumn7() {
        String tabName = "doubleNumberColumn7";
        String header = "allsalary,realsalary,alltax,tax1,tax2,tax3,tax4";

        String[] contents = ("10,7,2.5,0.5,1.55,0.50,\n" +
                "10,9,3.1,0.1,1.11,0.30,\n" +
                "12,10,3.2,0.2,1.22,0.22,\n" +
                "15,14,3.8,0.8,1.88,0.37,\n" +
                "15,13,4.7,0.7,1.77,0.95,\n" +
                "0,0,2.3,0.3,1.33,0.47,\n" +
                "0,0,2.1,0.1,1.11,0.36,\n" +
                "13,10,4.4,0.4,1.44,0.88,\n" +
                "12,11,2.6,0.6,1.66,0.99,\n" +
                "12,10,1.8,0.8,1.88,0.11,\n" +
                "12,,1.8,0.8,1.88,0.11,\n" +
                "12,10,1.8,0.8,,0.11,\n" +
                ",,,,,,").split("\n");

        File tab = FTableCreator.createCsv(header, contents);

        FTableInfo tableInfo = new FTableInfo(tabName, "tab01", tab.getAbsolutePath());
        for (String colName : header.split(",")) {
            tableInfo.addColumnInfo(new FColumnInfo(colName, FValueType.DOUBLE));
        }

        return tableInfo;
    }
}
