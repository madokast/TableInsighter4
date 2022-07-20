package com.sics.rock.tableinsight4.utils;


import scala.collection.immutable.Seq;
import scala.jdk.CollectionConverters;

import java.util.List;

/**
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
        return CollectionConverters.ListHasAsScala(es).asScala().toSeq();
    }

    public static <E> List<E> seqToJList(Seq<E> seq) {
        Iterable<E> es = CollectionConverters.IterableHasAsJava(seq).asJava();
        return FTiUtils.collect(es.iterator());
    }

}
