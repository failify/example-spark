package io.failify.examples.spark;

import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.ULimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.StringJoiner;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    public static Deployment getDeployment(int numOfSlaves) {
        String version = "2.4.3"; // this can be dynamically generated from maven metadata
        String dir = "spark-" + version + "-bin-custom-spark";
        Deployment.Builder builder = Deployment.builder("example-spark")
            .withService("spark-master")
                .applicationPath("../spark-2.4.3-build/" + dir + ".tar.gz", "/spark", PathAttr.COMPRESSED)
                .workDir("/spark/" + dir).startCommand("sbin/start-master.sh")
                .dockerImageName("failify/spark:1.0").dockerFileAddress("docker/Dockerfile", true)
                .logDirectory("/spark/" + dir + "/logs").and()
            .withService("spark-slave")
                .applicationPath("../spark-2.4.3-build/" + dir + ".tar.gz", "/spark", PathAttr.COMPRESSED)
                .workDir("/spark/" + dir).startCommand("sbin/start-slave.sh http://master:8080")
                .dockerImageName("failify/spark:1.0").dockerFileAddress("docker/Dockerfile", true)
                .logDirectory("/spark/" + dir + "/logs").and()
            .withNode("master", "spark-master").and();
        for (int i=1; i<=numOfSlaves; i++) builder.withNode("slave" + i, "spark-slave").and();
        return builder.build();
    }
}
