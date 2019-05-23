package io.failify.examples.spark;

import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.dsl.entities.PortType;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.ULimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.StringJoiner;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    public static Deployment getDeployment(int numOfMasters, int numOfSlaves) {
        String version = "2.4.3"; // this can be dynamically generated from maven metadata
        String dir = "spark-" + version + "-bin-custom-spark";
        Deployment.Builder builder = Deployment.builder("example-spark")
            .withService("zk").dockerImageName("zookeeper:3.4.14").disableClockDrift().and().withNode("zk1", "zk").and()
            .withService("spark-master")
                .applicationPath("../spark-2.4.3-build/" + dir + ".tar.gz", "/spark", PathAttr.COMPRESSED)
                .workDir("/spark/" + dir).startCommand("sbin/start-master.sh")
                .dockerImageName("failify/spark:1.0").dockerFileAddress("docker/Dockerfile", true)
                .logDirectory("/spark/" + dir + "/logs").and()
            .withService("spark-slave")
                .applicationPath("../spark-2.4.3-build/" + dir + ".tar.gz", "/spark", PathAttr.COMPRESSED)
                .workDir("/spark/" + dir).startCommand("sbin/start-slave.sh -c 1 -m 1G " + getMasterString(numOfMasters))
                .dockerImageName("failify/spark:1.0").dockerFileAddress("docker/Dockerfile", true)
                .logDirectory("/spark/" + dir + "/logs").and();
        for (int i=1; i<=numOfMasters; i++) builder.withNode("master" + i, "spark-master").offOnStartup()
                .tcpPort(7077).environmentVariable("SPARK_DAEMON_JAVA_OPTS",
                        "-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=zk1:2181 -Dzookeeper.sasl.client=false").and();
        for (int i=1; i<=numOfSlaves; i++) builder.withNode("slave" + i, "spark-slave").offOnStartup().and();
        return builder.build();
    }

    private static String getMasterString(int numOfMasters) {
        StringJoiner joiner = new StringJoiner(",");
        for (int i=1; i<=numOfMasters; i++) joiner.add("master" + i + ":7077");
        return "spark://" + joiner.toString();
    }

    public static String getClientMasterString(FailifyRunner runner, int numOfMasters) {
        StringJoiner joiner = new StringJoiner(",");
        for (int i=1; i<=numOfMasters; i++) joiner.add(runner.runtime().ip("master" + i) + ":" +
                runner.runtime().portMapping("master" + i, 7077, PortType.TCP));
        return "spark://" + joiner.toString();
    }

    public static void startNodesInOrder(FailifyRunner runner) throws InterruptedException, RuntimeEngineException {
        Thread.sleep(10000);
        for (String node: runner.runtime().nodeNames()) {
            if (node.startsWith("master"))
                runner.runtime().startNode(node);
        }
        Thread.sleep(10000);
        for (String node: runner.runtime().nodeNames()) {
            if (node.startsWith("slave"))
            runner.runtime().startNode(node);
        }
        Thread.sleep(10000);
    }
}
