package info.anber.pratice.spark.base;

import org.apache.spark.SparkConf;

public abstract class BaseRuntime {

    static {
        System.setProperty("HADOOP_HOME", "D:\\installtools\\hadoop-3.0.3");
    }

    protected static SparkConf getSparkConf(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local[*]");
        return sparkConf;
    }
}
