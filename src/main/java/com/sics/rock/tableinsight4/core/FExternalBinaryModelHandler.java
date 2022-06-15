package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.core.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.core.external.FExternalModelDerivedColumnAppender;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhaorx
 */
public class FExternalBinaryModelHandler implements FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FExternalBinaryModelHandler.class);

    public void appendDerivedColumn(FTableDatasetMap tables, List<FExternalBinaryModelInfo> externalBinaryModelInfos) {

        FExternalModelDerivedColumnAppender appender = new FExternalModelDerivedColumnAppender(
                config().externalBinaryModelDerivedColumnSuffix,
                config().idColumnName,
                spark()
        );

        externalBinaryModelInfos.forEach(ex -> appender.appendDerivedColumn(tables, ex));
    }
}
