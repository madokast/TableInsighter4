package com.sics.rock.tableinsight4.preprocessing;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelDerivedColumnAppender;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * external binary-model handler
 *
 * @author zhaorx
 */
public class FExternalBinaryModelHandler implements FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FExternalBinaryModelHandler.class);

    public void appendDerivedColumn(FTableDatasetMap tables, List<FExternalBinaryModelInfo> externalBinaryModelInfos) {

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        FExternalBinaryModelDerivedColumnAppender appender = new FExternalBinaryModelDerivedColumnAppender(
                derivedColumnNameHandler,
                config().idColumnName,
                spark()
        );

        externalBinaryModelInfos.forEach(ex -> appender.appendDerivedColumn(tables, ex));
    }
}
