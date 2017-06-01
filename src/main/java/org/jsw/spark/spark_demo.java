package org.jsw.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class spark_demo {

	private static JavaSparkContext sc;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        long all=0;
		SparkConf conf = new SparkConf().setAppName("firestsparkapp").setMaster("local");
		sc = new JavaSparkContext(conf);
		Consumer<? super Integer> println = null;

		JavaRDD<String> lines = sc
				.textFile("/home/develop/workspace/sparkdemo/fasttest_train.txt");

		// JavaRDD<Integer> lineLengths = lines.map(s -> s.length());

		JavaRDD words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String line) throws Exception {
				return  Arrays.asList(line.split(" ")).iterator();
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> wordCount = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		List<Tuple2<String, Integer>> list = wordCount.collect();
		for (Tuple2<String, Integer> pari : list) {
			System.out.println(pari._1() + ":" + pari._2());
			all+= pari._2();
		}
		System.out.println(all);
		sc.close();
		// int totalLength = lineLengths.reduce((a, b) -> a + b);
		// System.out.println(totalLength);
		// List<String> output = words.collect();
		// for (String tuple : output) {
		// System.out.println(tuple.intern());
		// }
		// System.out.println(lineLengths);
	}

}
