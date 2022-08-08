package com.sics.rock.tableinsight4.evidenceset;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class FBinaryLineEvidenceSetFactory implements Serializable {

    private final SparkSession spark;

    private final boolean sameTableFlag;


    public FIEvidenceSet build() {
        return null;
    }

    public FBinaryLineEvidenceSetFactory(SparkSession spark, boolean sameTableFlag) {
        this.spark = spark;
        this.sameTableFlag = sameTableFlag;
    }
}
