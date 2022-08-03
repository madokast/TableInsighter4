package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import javafx.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class FLocalPLIUtilsTest extends FBasicTestEnv {

    @Test
    public void localRowIdsOf() {

        Map<Integer, List<Integer>> left = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 2, 3),
                2, FTiUtils.listOf(4, 5, 6),
                3, FTiUtils.listOf(7, 8, 9)
        );

        Map<Integer, List<Integer>> right = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 5, 6),
                2, FTiUtils.listOf(7, 3, 4),
                4, FTiUtils.listOf(2, 8, 9)
        );


        FLocalPLI leftPLI = left.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        FLocalPLI rightPLI = right.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        long count = FLocalPLIUtils.localRowIdsOf(leftPLI, rightPLI, FOperator.EQ).peek(pair -> {
            List<Integer> l = pair.k();
            List<Integer> r = pair.v().collect(Collectors.toList());
            logger.info("left = {}, right = {}", l, r);

            if (l.contains(1)) assertTrue(r.contains(5));
            if (l.contains(4)) assertTrue(r.contains(7));
        }).count();

        assertEquals(2, count);

    }

    @Test
    public void localRowIdsOf2() {

        JavaRDD<Integer> rdd;

        Map<Integer, List<Integer>> left = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 2, 3),
                2, FTiUtils.listOf(4, 5, 6),
                3, FTiUtils.listOf(7, 8, 9)
        );

        Map<Integer, List<Integer>> right = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 5, 6),
                2, FTiUtils.listOf(7, 3, 4),
                4, FTiUtils.listOf(2, 8, 9)
        );


        FLocalPLI leftPLI = left.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        FLocalPLI rightPLI = right.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        logger.info("leftPLI = " + leftPLI);
        logger.info("rightPLI = " + rightPLI);

        List<Pair<List<Integer>, List<Integer>>> lr = FLocalPLIUtils.localRowIdsOf(leftPLI, rightPLI, FOperator.EQ).map(pair ->
                new Pair<>(pair.k(), pair.v().collect(Collectors.toList()))
        ).collect(Collectors.toList());


        List<Pair<List<Integer>, List<Integer>>> rl = FLocalPLIUtils.localRowIdsOf(rightPLI, leftPLI, FOperator.EQ).map(pair ->
                new Pair<>(pair.v().collect(Collectors.toList()), pair._k)
        ).collect(Collectors.toList());

        lr.forEach(e -> logger.info(e.toString()));
        rl.forEach(e -> logger.info(e.toString()));

        assertEquals(lr, rl);
    }

    @Test
    public void localRowIdsOf_lt() {

        Map<Integer, List<Integer>> left = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 2, 3),
                2, FTiUtils.listOf(4, 5, 6),
                3, FTiUtils.listOf(7, 8, 9)
        );

        Map<Integer, List<Integer>> right = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 5, 6),
                2, FTiUtils.listOf(7, 3, 4),
                4, FTiUtils.listOf(2, 8, 9)
        );


        FLocalPLI leftPLI = left.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        FLocalPLI rightPLI = right.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        long count = FLocalPLIUtils.localRowIdsOf(leftPLI, rightPLI, FOperator.LT).peek(pair -> {
            List<Integer> l = pair.k();
            List<Integer> r = pair.v().collect(Collectors.toList());

            if (l.contains(1)) assertTrue(r.contains(7));
            if (l.contains(1)) assertTrue(r.contains(8));
            if (l.contains(1)) assertFalse(r.contains(1));

            if (l.contains(5)) assertTrue(r.contains(9));
            if (l.contains(5)) assertFalse(r.contains(7));
            if (l.contains(5)) assertFalse(r.contains(1));


            if (l.contains(9)) assertTrue(r.contains(9));
            if (l.contains(9)) assertFalse(r.contains(7));
            if (l.contains(9)) assertFalse(r.contains(1));

            logger.info("l = {}, r = {}", l, r);
        }).count();

        assertEquals(3, count);
    }


    @Test
    public void localRowIdsOf_gt() {

        Map<Integer, List<Integer>> left = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 2, 3),
                2, FTiUtils.listOf(4, 5, 6),
                3, FTiUtils.listOf(7, 8, 9),
                -1, FTiUtils.listOf(17, 18, 19),
                -2, FTiUtils.listOf(27, 28, 29),
                -3, FTiUtils.listOf(37, 38, 39)
        );

        Map<Integer, List<Integer>> right = FTiUtils.mapOf(
                1, FTiUtils.listOf(1, 5, 6),
                2, FTiUtils.listOf(7, 3, 4),
                4, FTiUtils.listOf(2, 8, 9),
                -1, FTiUtils.listOf(17, 18, 19),
                -2, FTiUtils.listOf(27, 28, 29),
                -3, FTiUtils.listOf(37, 38, 39)
        );


        FLocalPLI leftPLI = left.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        FLocalPLI rightPLI = right.entrySet().stream().flatMap(
                e -> e.getValue().stream().map(r -> FLocalPLI.singleLinePLI(1, e.getKey().longValue(), r)))
                .reduce(FLocalPLI::merge).get();

        long count = FLocalPLIUtils.localRowIdsOf(leftPLI, rightPLI, FOperator.GT).peek(pair -> {
            List<Integer> l = pair.k();
            List<Integer> r = pair.v().collect(Collectors.toList());

            if (l.contains(1)) assertTrue(r.isEmpty());
            if (l.contains(2)) assertTrue(r.isEmpty());
            if (l.contains(3)) assertTrue(r.isEmpty());

            if (l.contains(4)) assertTrue(r.contains(1));
            if (l.contains(5)) assertTrue(r.contains(5));
            if (l.contains(6)) assertTrue(r.contains(6));

            if (l.contains(6)) assertFalse(r.contains(9));


            if (l.contains(7)) assertEquals(6, r.size());
            if (l.contains(8)) assertEquals(6, r.size());
            if (l.contains(9)) assertEquals(6, r.size());

            logger.info("l = {}, r = {}", l, r);
        }).count();

        assertEquals(3, count);
    }
}