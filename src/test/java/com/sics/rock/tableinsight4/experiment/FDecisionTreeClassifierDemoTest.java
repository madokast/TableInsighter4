package com.sics.rock.tableinsight4.experiment;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.test.FTableCreator;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.tree.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

public class FDecisionTreeClassifierDemoTest extends FSparkEnv {

    @Test
    public void demo() {
        // label column
        final String pass = "pass";
        // 3 feature columns
        final String score = "score";
        final String homework = "homework";
        final String attendance = "attendance";

        // make data
        final String header = String.join(",", pass, score, homework, attendance);
        final File csv = FTableCreator.createCsv(header, randData(100000));

        // create schema
        final StructType schema = new StructType()
                .add(pass, DataTypes.StringType, false)
                .add(score, DataTypes.DoubleType, false)
                .add(homework, DataTypes.StringType, false)
                .add(attendance, DataTypes.IntegerType, false);

        // load data
        //+----+-----+--------+----------+
        //|pass|score|homework|attendance|
        //+----+-----+--------+----------+
        //|  no| 14.0|  normal|         0|
        //|  no| 81.0|     bad|         2|
        final Dataset<Row> table = spark.read().schema(schema).option("header", "true").csv(csv.getAbsolutePath());
        table.show(5, false);

        // index the string columns "pass" and "homework"
        //|pass|score|homework|attendance|passIndex|homeworkIndex|
        //+----+-----+--------+----------+---------+-------------+
        //| yes| 68.0|  normal|         1|      1.0|          1.0|
        //|  no| 57.0|     bad|         0|      0.0|          0.0|
        //|  no| 16.0|    good|         2|      0.0|          2.0|
        final String label = pass + "Index";
        final String feature0 = score;
        final String feature1 = homework + "Index";
        final String feature2 = attendance;
        final StringIndexerModel passIndexer = new StringIndexer().setInputCol(pass).setOutputCol(label).fit(table);
        final StringIndexerModel homeworkIndexer = new StringIndexer().setInputCol(homework).setOutputCol(feature1).fit(table);
        final Dataset<Row> tableIndexed = homeworkIndexer.transform(passIndexer.transform(table));
        tableIndexed.show(5, false);

        // create features-vec
        //|pass|score|homework|attendance|passIndex|homeworkIndex|      features|
        //+----+-----+--------+----------+---------+-------------+--------------+
        //|  no|  8.0|     bad|         0|      0.0|          0.0| [8.0,0.0,0.0]|
        //| yes| 70.0|    good|         2|      1.0|          1.0|[70.0,1.0,2.0]|
        //|  no| 33.0|    good|         0|      0.0|          1.0|[33.0,1.0,0.0]|
        final String features = "features";
        final VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(new String[]{feature0, feature1, feature2}).setOutputCol(features);
        final Dataset<Row> tableIndexedFeature = vectorAssembler.transform(tableIndexed);
        tableIndexedFeature.show(5, false);


        // index the features-vec
        //|pass|score|homework|attendance|passIndex|homeworkIndex|      features| featuresIndex|
        //+----+-----+--------+----------+---------+-------------+--------------+--------------+
        //| yes| 87.0|    good|         0|      1.0|          1.0|[87.0,1.0,0.0]|[87.0,1.0,0.0]|
        //|  no|  3.0|     bad|         1|      0.0|          0.0| [3.0,0.0,1.0]| [3.0,0.0,1.0]|
        final String featuresIndex = features + "Index";
        final VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol(features).setOutputCol(featuresIndex).setMaxCategories(5).fit(tableIndexedFeature);
        final Dataset<Row> tableIndexedFeatureIndex = featureIndexer.transform(tableIndexedFeature);
        tableIndexedFeatureIndex.show(5, false);

        // model
        final org.apache.spark.ml.classification.DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol(label)
                .setFeaturesCol(featuresIndex)
                .setImpurity("entropy")  // "entropy", "gini"
                .setMaxBins(10)  // Max number of bins for discretizing continuous features
                .setMaxDepth(5)  // max depth of tree
                .setMinInfoGain(0.01)  // Minimum information gain for a split to be considered at a tree node.
                .setMinInstancesPerNode(5)  // Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid.
                ;

        // convert the predicate result (in index form) to original label. The inverse transformation of passIndexer
        final IndexToString labelPassConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedPass").setLabels(passIndexer.labels());
        final DecisionTreeClassificationModel model = dt.fit(tableIndexedFeatureIndex);

        // use trained model
        //|pass|score|homework|attendance|passIndex|homeworkIndex|features      |featuresIndex |rawPrediction|probability                              |prediction|predictedPass|
        //+----+-----+--------+----------+---------+-------------+--------------+--------------+-------------+-----------------------------------------+----------+-------------+
        //|no  |64.0 |bad     |1         |0.0      |2.0          |[64.0,2.0,1.0]|[64.0,2.0,1.0]|[9.0,2.0]    |[0.8181818181818182,0.18181818181818182] |0.0       |no           |
        //|yes |79.0 |good    |0         |1.0      |0.0          |[79.0,0.0,0.0]|[79.0,0.0,0.0]|[2.0,27.0]   |[0.06896551724137931,0.9310344827586207] |1.0       |yes          |
        final Dataset<Row> result = labelPassConverter.transform(model.transform(tableIndexedFeatureIndex));
        result.show(false);

        // print tree
        //  If (feature 0 <= 59.5)
        //   Predict: 0.0
        //  Else (feature 0 > 59.5)
        //   If (feature 1 in {0.0})
        //    If (feature 0 <= 89.5)
        //     Predict: 0.0
        //    Else (feature 0 > 89.5)
        //     Predict: 1.0
        //   Else (feature 1 not in {0.0})
        //    If (feature 2 in {0.0})
        //     If (feature 1 in {1.0})
        //      If (feature 0 <= 89.5)
        //       Predict: 0.0
        //      Else (feature 0 > 89.5)
        //       Predict: 1.0
        //     Else (feature 1 not in {1.0})
        //      Predict: 1.0
        //    Else (feature 2 not in {0.0})
        //     Predict: 1.0
        logger.info(model.toDebugString());

        //09:57:44.331 [main] INFO  TEST - Feature(0) impurity[0.8223233167690127] split = 59.5
        //09:57:44.331 [main] DEBUG TEST - visiting leaf node
        //09:57:44.332 [main] INFO  TEST - Feature(1) impurity[0.9408729857346512] split = [2.0]|[0.0, 1.0]
        //09:57:44.332 [main] INFO  TEST - Feature(0) impurity[0.7706204773916131] split = 89.5
        //09:57:44.332 [main] DEBUG TEST - visiting leaf node
        //09:57:44.332 [main] DEBUG TEST - visiting leaf node
        //09:57:44.333 [main] INFO  TEST - Feature(2) impurity[0.6088031505191881] split = [0.0]|[1.0, 2.0]
        //09:57:44.333 [main] INFO  TEST - Feature(1) impurity[0.9722944247504209] split = [1.0]|[0.0, 2.0]
        //09:57:44.333 [main] INFO  TEST - Feature(0) impurity[0.7535884078560637] split = 89.5
        //09:57:44.333 [main] DEBUG TEST - visiting leaf node
        //09:57:44.333 [main] DEBUG TEST - visiting leaf node
        //09:57:44.333 [main] DEBUG TEST - visiting leaf node
        //09:57:44.333 [main] DEBUG TEST - visiting leaf node
        traverseTree(model.rootNode());


    }

    private void traverseTree(Node node) {
        if (node instanceof InternalNode) {
            final double impurity = node.impurity();
            final Split split = ((InternalNode) node).split();
            final int featureIndex = split.featureIndex();
            final String splitStr;
            if (split instanceof ContinuousSplit) {
                final double threshold = ((ContinuousSplit) split).threshold();
                splitStr = Double.toString(threshold);
            } else if (split instanceof CategoricalSplit) {
                final double[] leftCategories = ((CategoricalSplit) split).leftCategories();
                final double[] rightCategories = ((CategoricalSplit) split).rightCategories();
                splitStr = Arrays.toString(leftCategories) + "|" + Arrays.toString(rightCategories);
            } else {
                logger.error("unknown split: " + split.getClass() + " " + split.toString());
                splitStr = split.toString();
            }
            logger.info("Feature({}) impurity[{}] split = {}", featureIndex, impurity, splitStr);

            final Node leftChild = ((InternalNode) node).leftChild();
            final Node rightChild = ((InternalNode) node).rightChild();
            traverseTree(leftChild);
            traverseTree(rightChild);
        } else if (node instanceof LeafNode) {
            logger.debug("visiting leaf node");
        } else if (node != null) {
            logger.error("unknown tree node: " + node.getClass() + " " + node.toString());
        }
    }

    // score 0-100
    // homework good normal bad
    // attendance 0-3
    private boolean pass(double score, String homework, int attendance) {
        if (score > 90) {
            return true;
        } else if (score > 60) {
            return (homework.equals("good")) || (homework.equals("normal") && attendance > 0) || (homework.equals("bad") && attendance > 2);
        } else if (score > 40) {
            return homework.equals("good") && attendance > 2;
        } else {
            return false;
        }
    }

    private String[] randData(int number) {
        final Random random = new Random();
        final String[] homeworkEnum = {"good", "normal", "bad"};
        return IntStream.range(0, number).mapToObj(i -> {
            final double score = random.nextInt(100);
            final String homework = homeworkEnum[random.nextInt(3)];
            final int attendance = random.nextInt(3);
            final String pass = pass(score, homework, attendance) ? "yes" : "no";
            return String.format("%s,%f,%s,%d", pass, score, homework, attendance);
        }).toArray(String[]::new);
    }


}