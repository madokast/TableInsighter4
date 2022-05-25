package com.sics.rock.tableinsight4.test;

import java.io.File;

public class FTableCreator {

    private static final String NEW_LINE = System.lineSeparator();

    public static File createCsv(String header, String... lines) {
        FTempFile tab = FTempFile.create(header + NEW_LINE + String.join(NEW_LINE, lines), ".csv");
        return tab.getPath();

    }
}
