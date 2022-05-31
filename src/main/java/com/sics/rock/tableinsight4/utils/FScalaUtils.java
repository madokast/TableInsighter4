package com.sics.rock.tableinsight4.utils;

import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;
import scala.collection.mutable.ListBuffer;
import scala.jdk.CollectionConverters;

public class FScalaUtils {

    public static <E> Seq<E> seqOf(E... es) {
        return CollectionConverters.ListHasAsScala(FUtils.listOf(es)).asScala().toSeq();


//        ListBuffer<E> r = new ListBuffer<>();
//        for (E e : es) {
//            r.addOne(e);
//        }
//        return r.toSeq();
    }

}
