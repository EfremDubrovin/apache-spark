package eu.deltasource;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class SparkOperationsExamples {

	public static void main(String[] args) {
		// Initialize Spark
		SparkConf conf = new SparkConf()
			.setAppName("test_sum")
			.setMaster("local[*]");// what local and what is the asterisk?
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Parallelized collections
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);

		// Reduce operation
		Integer sum = distData.reduce(Integer::sum);
		log.info("The sum of elements is: {}", sum);

		JavaRDD<Double> squareRoots = distData.map(value -> Math.sqrt(value));
		squareRoots.foreach(squareRoot -> log.info("root value {}", squareRoot));

		// counting the elements in an RDD?
		long count = squareRoots.count();
		log.info("Number of elements: {}", count);

		sc.close();
	}
}
