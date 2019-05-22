package io.failify.examples.spark;

import io.failify.FailifyRunner;
import io.failify.exceptions.RuntimeEngineException;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.TimeoutException;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);

    protected static FailifyRunner runner;
    protected static final int NUM_OF_SLAVES = 3;
    protected static final int NUM_OF_MASTERS = 3;

    @BeforeClass
    public static void before() throws RuntimeEngineException, InterruptedException {
        runner = FailifyHelper.getDeployment(NUM_OF_MASTERS, NUM_OF_SLAVES).start();
        FailifyHelper.startNodesInOrder(runner);


        logger.info("The cluster is UP!");
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }


    @Test
    public void sampleTest() throws RuntimeEngineException, SQLException, ClassNotFoundException, TimeoutException {
        String logFile = "README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().master(FailifyHelper.getClientMasterString(runner, NUM_OF_MASTERS))
                .appName("Simple Application").getOrCreate();

        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
