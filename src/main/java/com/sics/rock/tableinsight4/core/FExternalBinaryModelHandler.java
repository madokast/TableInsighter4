package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.core.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.core.external.FExternalModelDerivedColumnAppender;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhaorx
 */
public class FExternalBinaryModelHandler implements FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FExternalBinaryModelHandler.class);

    public void appendDerivedColumn(FTableDatasetMap tables, List<FExternalBinaryModelInfo> externalBinaryModelInfos) {

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        FExternalModelDerivedColumnAppender appender = new FExternalModelDerivedColumnAppender(
                derivedColumnNameHandler,
                config().idColumnName,
                spark()
        );

        externalBinaryModelInfos.forEach(ex -> appender.appendDerivedColumn(tables, ex));
    }
}
