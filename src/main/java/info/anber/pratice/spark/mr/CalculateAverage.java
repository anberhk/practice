package info.anber.pratice.spark.mr;

import info.anber.pratice.spark.base.BaseRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculate average of list, 
 *
 */
public class CalculateAverage extends BaseRuntime {

    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(
                getSparkConf("Calculate average application"));
        List<Integer> scores = new ArrayList<>();
        for (int i =0 ;i <= 10; i++) {
            scores.add(i);
        }
        JavaRDD<Integer> scoreRDD = javaSparkContext.parallelize(scores);
        JavaPairRDD<Integer, Integer> targetRDD = scoreRDD.mapToPair(
                score -> new Tuple2<>(1, score));
        Tuple2<Integer, Integer> numRDD = targetRDD.reduce((preTuple, tuple) ->
            new Tuple2<>(preTuple._1() + tuple._1(), preTuple._2() + tuple._2())
        );
        System.out.println(numRDD._2() / numRDD._1());
    }

}
