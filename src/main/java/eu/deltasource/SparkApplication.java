package eu.deltasource;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class SparkApplication {

	public static void main(String[] args) {
		// Initialize Spark
		SparkConf conf = new SparkConf()
//			.setJars(new String[]{"target/spark.jar"})
			.setAppName("test_sum")
//			.setMaster("spark://spark:7077");
			.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Parallelized collections
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);

		Integer sum = distData.reduce(Integer::sum);
		log.info("The sum of elements is: {}", sum);

		// External dataset
//		JavaRDD<String> lines = sc.textFile("/bitnami/spark/data.txt");
		JavaRDD<String> lines = sc.textFile("/Users/efrem/Documents/project/spark/src/main/resources/data.txt");
		// Note data.txt should be accessible from all worker nodes (see mounted volumes)
		JavaRDD<Integer> lineLengths = lines.map(String::length);
		int totalLength = lineLengths.reduce(Integer::sum);

		log.info("The sum of all characters in the file: {}", totalLength);
	}
}
