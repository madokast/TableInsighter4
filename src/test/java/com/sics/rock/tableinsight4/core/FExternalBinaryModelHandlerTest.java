package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.core.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.core.external.FIExternalBianryModelPredicateNameFormatter;
import com.sics.rock.tableinsight4.core.external.FIExternalBinaryModelCalculator;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import com.sics.rock.tableinsight4.utils.FScalaUtils;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FExternalBinaryModelHandlerTest extends FTableInsightEnv {

    private static final String idCol = "row_id";

    @Test
    public void test_appendDerivedColumn() {

        final FExternalBinaryModelHandler handler = new FExternalBinaryModelHandler(
                spark, idCol, "$EX_");

        final FTableDatasetMap relation = relation();

        final FExternalBinaryModelInfo info = similar(relation);

        handler.appendDerivedColumn(relation, Collections.singletonList(info));

        relation.getDatasetByTableName("relation").show();

    }

    private FExternalBinaryModelInfo similar(FTableDatasetMap relation) {
        final String tabName = "relation";
        final Dataset<Row> dataset = relation.getDatasetByTableName(tabName);

        dataset.show();

        List<FPair<Long, Long>> pairs = new ArrayList<>();

        final List<Row> rows = dataset.select("cc", FScalaUtils.seqOf("row_id")).toJavaRDD().collect();
        for (int i = 0; i < rows.size(); i++) {
            for (int j = i + 1; j < rows.size(); j++) {
                final Row r1 = rows.get(i);
                final Row r2 = rows.get(j);
                if (r1.get(0).equals(r2.get(0))) {
                    final FPair<Long, Long> pair = new FPair<>(r1.getLong(1), r2.getLong(1));
                    logger.debug("Pair {}", pair);
                    pairs.add(pair);
                }
            }
        }

        final String id = "cc_similar";
        final FIExternalBinaryModelCalculator calculator = () -> pairs;
        final FIExternalBianryModelPredicateNameFormatter predicateNameFormatter = (i, j) -> "Similar('eq', t0.cc = t1.cc)";


        return new FExternalBinaryModelInfo(id, tabName, tabName,
                true, calculator, predicateNameFormatter);
    }

    private FTableDatasetMap relation() {

        final FTableInfo relationInfo = FExamples.relation();

        final FTableDataLoader dataLoader = new FTableDataLoader();

        return dataLoader.prepareData(Collections.singletonList(relationInfo));
    }

    // ------------------ cross table ------------------

    @Test
    public void test_appendDerivedColumn2() {

        final FExternalBinaryModelHandler handler = new FExternalBinaryModelHandler(
                spark, idCol, "$EX_");

        final FTableDatasetMap relation = relation2();

        final FExternalBinaryModelInfo info = similar2(relation);

        handler.appendDerivedColumn(relation, Collections.singletonList(info));

        relation.getDatasetByTableName("r1").show();
        relation.getDatasetByTableName("r2").show();

    }

    private FExternalBinaryModelInfo similar2(FTableDatasetMap relation) {
        final Dataset<Row> d1 = relation.getDatasetByTableName("r1");
        final Dataset<Row> d2 = relation.getDatasetByTableName("r2");

        List<FPair<Long, Long>> pairs = new ArrayList<>();

        final List<Row> rows1 = d1.select("cc", FScalaUtils.seqOf("row_id")).toJavaRDD().collect();
        final List<Row> rows2 = d2.select("cc", FScalaUtils.seqOf("row_id")).toJavaRDD().collect();
        for (int i = 0; i < rows1.size(); i++) {
            for (int j = 0; j < rows2.size(); j++) {
                final Row r1 = rows1.get(i);
                final Row r2 = rows2.get(j);
                if (r1.get(0).equals(r2.get(0))) {
                    final FPair<Long, Long> pair = new FPair<>(r1.getLong(1), r2.getLong(1));
                    logger.debug("Pair {}", pair);
                    pairs.add(pair);
                }
            }
        }

        final String id = "cc_similar";
        final FIExternalBinaryModelCalculator calculator = () -> pairs;
        final FIExternalBianryModelPredicateNameFormatter predicateNameFormatter = (i, j) -> "Similar('eq', t0.cc = t2.cc)";


        return new FExternalBinaryModelInfo(id, "r1", "r2",
                true, calculator, predicateNameFormatter);
    }

    private FTableDatasetMap relation2() {

        final FTableInfo r1 = FExamples.relation("r1", "r1");
        final FTableInfo r2 = FExamples.relation("r2", "r2");


        final FTableDataLoader dataLoader = new FTableDataLoader();

        return dataLoader.prepareData(FUtils.listOf(r1, r2));
    }
}