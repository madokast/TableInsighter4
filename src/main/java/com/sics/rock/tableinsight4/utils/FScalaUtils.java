package com.sics.rock.tableinsight4.utils;


import scala.collection.immutable.Seq;
import scala.jdk.CollectionConverters;

import java.util.List;

/**
 * java <==> scala utils
 *
 * @author zhaorx
 */
public class FScalaUtils {

    @SafeVarargs
    public static <E> Seq<E> seqOf(E... es) {
        return seqOf(FTiUtils.listOf(es));

//        ListBuffer<E> r = new ListBuffer<>();
//        for (E e : es) {
//            r.addOne(e);
//        }
//        return r.toSeq();
    }

    public static <E> Seq<E> seqOf(List<E> es) {
        // scala 2.13
        return CollectionConverters.ListHasAsScala(es).asScala().toSeq();
        // scala 2.12
        // return JavaConversions.asScalaBuffer(es).toList().seq();
    }

    public static <E> List<E> seqToJList(Seq<E> seq) {
        // scala 2.13
        Iterable<E> es = CollectionConverters.IterableHasAsJava(seq).asJava();
        return FTiUtils.collect(es.iterator());

        // final Iterable<E> es = JavaConversions.asJavaIterable(seq);
        // List<E> r = new ArrayList<>();
        // for (final E e : es) {
        //     r.add(e);
        // }
        // return r;
    }

    // scala 2.12
    //    public static <E> List<E> seqToJList(final scala.collection.Seq<E> seq) {
    //        final Iterable<E> es = JavaConversions.asJavaIterable(seq);
    //        List<E> r = new ArrayList<>();
    //        for (final E e : es) {
    //            r.add(e);
    //        }
    //        return r;
    //    }
}
