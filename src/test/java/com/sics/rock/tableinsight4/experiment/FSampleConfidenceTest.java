package com.sics.rock.tableinsight4.experiment;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;

public class FSampleConfidenceTest extends FBasicTestEnv {

    @Test
    public void test01() {
        int sampleSize = 3;
        int appearTime = 2;

        double lowerBound = 0.5;

        MathContext mc = new MathContext(2500, RoundingMode.HALF_EVEN);
        Function<Double, BigDecimal> fun = p -> {
            BigDecimal pp = BigDecimal.valueOf(p);
            // p ^ at * (1-p) ^ sat
            return pp.pow(appearTime, mc)
                    .multiply(
                            (BigDecimal.ONE.subtract(pp)).pow(sampleSize - appearTime, mc)
                    );
        };

        BigDecimal norm = integral(0, 1, 1000, fun, mc);

        logger.info("norm = " + norm);

        BigDecimal prob = integral(lowerBound, 1, 1000, fun, mc);

        logger.info("prob before norm= " + prob);

        prob = prob.divide(norm, mc);

        logger.info("prob = " + prob);
    }

//    @Test
    public void test02() {
        int sampleSize = 100;
        int appearTime = 8;

        double lowerBound = 0.05;

        MathContext mc = new MathContext(2500, RoundingMode.HALF_EVEN);
        Function<Double, BigDecimal> fun = p -> {
            BigDecimal pp = BigDecimal.valueOf(p);
            // p ^ at * (1-p) ^ sat
            return pp.pow(appearTime, mc)
                    .multiply(
                            (BigDecimal.ONE.subtract(pp)).pow(sampleSize - appearTime, mc)
                    );
        };

        BigDecimal norm = integral(0, 1, 5000, fun, mc);

        logger.info("norm = " + norm);

        BigDecimal prob = integral(lowerBound, 1, 5000, fun, mc);

        logger.info("prob before norm= " + prob);

        prob = prob.divide(norm, mc);

        logger.info("prob = " + prob);
    }

//    @Test
    public void test03() {

        for (int appearTime = 0; appearTime < 30; appearTime++) {

            int sampleSize = 300;
            double lowerBound = 0.05;

            MathContext mc = new MathContext(220, RoundingMode.HALF_EVEN);
            int finalAppearTime = appearTime;
            Function<Double, BigDecimal> fun = p -> {
                BigDecimal pp = BigDecimal.valueOf(p);
                // p ^ at * (1-p) ^ sat
                return pp.pow(finalAppearTime, mc)
                        .multiply(
                                (BigDecimal.ONE.subtract(pp)).pow(sampleSize - finalAppearTime, mc)
                        );
            };

            BigDecimal norm = integral(0, 1, 5000, fun, mc);
            BigDecimal prob = integral(lowerBound, 1, 5000, fun, mc);

            prob = prob.divide(norm, mc);

            logger.info("appear = {} prob = {}", appearTime, prob.doubleValue());
        }
    }

//    @Test
    public void test04() {

        for (int appearTime = 80; appearTime < 81; appearTime++) {

            int sampleSize = 10000;
            double lowerBound = 0.01;

            MathContext mc = new MathContext(21000, RoundingMode.HALF_EVEN);
            int finalAppearTime = appearTime;
            Function<Double, BigDecimal> fun = p -> {
                BigDecimal pp = BigDecimal.valueOf(p);
                // p ^ at * (1-p) ^ sat
                return pp.pow(finalAppearTime, mc)
                        .multiply(
                                (BigDecimal.ONE.subtract(pp)).pow(sampleSize - finalAppearTime, mc)
                        );
            };

            //0.0224170595210779   5000
            //0.023015336192745536 3000
            //0.022211535178888218 8000
            BigDecimal norm = integral(0, 1, 8000, fun, mc);
            BigDecimal prob = integral(lowerBound, 1, 8000, fun, mc);

            prob = prob.divide(norm, mc);

            logger.info("appear = {} prob = {}", appearTime, prob.doubleValue());
        }
    }

    private static BigDecimal integral(double lower, double upper, int number, Function<Double, BigDecimal> fun, MathContext mc) {
        assert upper > lower;
        double[] dots = linSpace(lower, upper, number);
        double interval = dots[1] - dots[0];

//        BigDecimal ret = BigDecimal.ZERO;

        return IntStream.range(1, number).parallel().mapToObj(i -> {
            BigDecimal left = fun.apply(dots[i - 1]);
            BigDecimal right = fun.apply(dots[i]);

            BigDecimal avg = left.add(right).divide(BigDecimal.valueOf(2), mc);
            return avg.multiply(BigDecimal.valueOf(interval), mc);
        }).reduce(BigDecimal::add).get();
    }

    private static double[] linSpace(double lower, double upper, int number) {
        assert number > 1;
        double interval = (upper - lower) / (number - 1);
        double[] ret = new double[number];
        for (int i = 0; i < number; i++) {
            ret[i] = lower + interval * i;
        }
        return ret;
    }

    private static BigInteger combine(int n, int m) {
        BigInteger ret = BigInteger.ONE;

        for (int i = 0; i < m; i++) {
            ret = ret.multiply(BigInteger.valueOf(n - i));
        }

        for (int i = 0; i < m; i++) {
            ret = ret.divide(BigInteger.valueOf(m - i));
        }

        return ret;
    }

    @Test
    public void test_integral() {
        logger.info("x| 0-1 = {}", integral(
                0, 1, 100, BigDecimal::valueOf, new MathContext(100, RoundingMode.HALF_EVEN)
        ));
    }

    @Test
    public void test_linSpace() {
        logger.info("(0,1,10) = {}", Arrays.toString(linSpace(0, 1, 10)));
    }

    @Test
    public void test_combine() {
        logger.info("combine(6, 0) = {}", combine(6, 0));
        logger.info("combine(6, 1) = {}", combine(6, 1));
        logger.info("combine(6, 2) = {}", combine(6, 2));
        logger.info("combine(6, 3) = {}", combine(6, 3));
        logger.info("combine(6, 4) = {}", combine(6, 4));
        logger.info("combine(6, 5) = {}", combine(6, 5));
        logger.info("combine(6, 6) = {}", combine(6, 6));

        Assert.assertEquals(1, combine(6, 0).intValue());
        Assert.assertEquals(6, combine(6, 1).intValue());
        Assert.assertEquals(15, combine(6, 2).intValue());
        Assert.assertEquals(20, combine(6, 3).intValue());
        Assert.assertEquals(15, combine(6, 4).intValue());
        Assert.assertEquals(6, combine(6, 5).intValue());
        Assert.assertEquals(1, combine(6, 6).intValue());
    }
}
