package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zhaorx
 */
public class FKMeansUtils {


    public static List<FPair<Double, Double>> findRanges(JavaRDD<Double> doubleRDD, int clusterNumber, int iterNumber) {
        final List<Double> boundaries = findBoundariesByKMeans(doubleRDD, clusterNumber, iterNumber);

        // -inf +inf
        if (boundaries.size() <= 2) return Collections.emptyList();

        List<FPair<Double, Double>> ranges = new ArrayList<>();
        for (int bid = 1; bid < boundaries.size(); bid++) {
            final Double left = boundaries.get(bid - 1);
            final Double right = boundaries.get(bid);
            if (left.equals(right)) continue;
            ranges.add(new FPair<>(left, right));
        }

        return ranges;
    }


    public static List<Double> findBoundariesByKMeans(JavaRDD<Double> doubleRDD, int clusterNumber, int iterNumber) {
        final RDD<Vector> data = doubleRDD
                .map(num -> Vectors.dense(new double[]{num}))
                .rdd()
                .cache()
                .setName("KMeans_" + System.currentTimeMillis());

        final KMeansModel result = KMeans.train(data, clusterNumber, iterNumber);

        final Vector[] centers = result.clusterCenters();


        final List<Double> boundaries = new ArrayList<>(centers.length + 1);
        boundaries.add(Double.NEGATIVE_INFINITY); // -inf
        for (int cid = 0; cid < centers.length - 1; cid++) {
            final double left = centers[cid].apply(0);
            final double right = centers[cid + 1].apply(0);
            final double boundary = (left + right) / 2;

            boundaries.add(boundary);
        }
        boundaries.add(Double.POSITIVE_INFINITY); // +inf

        boundaries.sort(Double::compareTo);

        return boundaries;
    }
}
