package com.sics.rock.tableinsight4.evidenceset.factory;

import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FBinaryLineEvidenceSetFactoryTest extends FTableInsightEnv {

    @Test
    public void createSingleTableBinaryLineEvidenceSet() {

        config().sliceLengthForPLI = 2;

        final FTableInfo table = FExamples.create("two-row", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.STRING}, new String[]{
                        "zhangsan,26",
                        "zhangsan,26",
                        "zhangsan,26",
                        "zhangsan,24"
                });

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(table));

        final List<FExternalBinaryModelInfo> externalBinaryModelInfos = Collections.emptyList();
        final FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

        final FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        final FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        final FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        final FPLI PLI = pliConstructor.construct(tableDatasetMap);

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        final FPredicateIndexer singleLinePredicateFactory = FPredicateFactory.createSingleTableCrossLinePredicates(
                table, false, derivedColumnNameHandler, Collections.emptyList());

        for (FIPredicate predicate : singleLinePredicateFactory.allPredicates()) {
            logger.info("predicate = " + predicate);
        }

        final int p_name_i = singleLinePredicateFactory.findIndex("name").get(0);
        final int p_age_i = singleLinePredicateFactory.findIndex("age").get(0);

        final FBinaryLineEvidenceSetFactory evidenceSetFactory = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory();

        final FIEvidenceSet ES = evidenceSetFactory.createSingleTableBinaryLineEvidenceSet(
                table, PLI, singleLinePredicateFactory, table.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();

            if (bs.get(p_age_i)) Assert.assertTrue(bs.get(p_name_i));
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }

    }
}