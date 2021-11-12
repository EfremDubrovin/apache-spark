package eu.deltasource;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class PairRddOperationsExamples {

	public static void main(String[] args) {
		// Initialize Spark
		SparkConf conf = new SparkConf()
			.setAppName("test_pair_rdds")
			.setMaster("local[*]");// what local and what is the asterisk?
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> logs = Arrays.asList(
			"INFO : Converting all alias Ids to primary Ids",
			"INFO : Starting PrimaryIdsDuplication validation",
			"INFO : PrimaryIdsDuplication validation completed.",
			"WARN : Failed secondaryId validation of uniqueness and immutability for 'Title' with id: [ scope: 'mm', value: 'h/335435'] and secondaryId: [ type: 'SipadanCRID', value: 'crid://magnetmedia.be/show/335435']",
			"WARN : HHH000104: firstResult/maxResults specified with collection fetch; applying in memory!",
			"ERROR: Total events = 373, duplicated events = 68. Duplicated events are 18.23% of the total events, that exceeds the threshold of 2.00%.",
			"ERROR: Total events = 379, duplicated events = 68. Duplicated events are 17.94% of the total events, that exceeds the threshold of 2.00%",
			"ERROR: Total events = 1053, duplicated events = 22. Duplicated events are 2.09% of the total events, that exceeds the threshold of 2.00%.");

		// Question: difference between Java Map and PairRDD?

		JavaPairRDD<String, String> logsByLevel = sc.parallelize(logs)
			.mapToPair(logLine -> {
				String[] columns = logLine.split(":");
				String level = columns[0].trim();
				String message = columns[1];
				return new Tuple2<>(level, message);
			});

		JavaPairRDD<String, Integer> mappedTuples = logsByLevel.mapToPair(tuple -> new Tuple2<>(tuple._1, 1));

		JavaPairRDD<String, Integer> groupedTuples = mappedTuples.reduceByKey((v1, v2) -> v1 + v2);

		groupedTuples.foreach(tuple -> log.info("Total {} - {}", tuple._1, tuple._2));

		// Next 15 minutes homework -> Calculate the sum of ERROR Total events which have exceeded the threshold
		sc.close();
	}
}
