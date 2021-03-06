package com.ba.spark.tutorial;

import static com.ba.spark.tutorial.CommontUtils.getIndicators;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import scala.Tuple2;

/**
 * Created by bartyushenko on 27.07.16.
 */
public class MultipleJenkinsJ8 extends MultipleJenkinsSparkKM {

	public static void main(String[] strings) throws Exception {
		boolean printed = false;

		try (PrintStream out = new PrintStream(
				Files.newOutputStream(Paths.get("/work/coe/spark_jenkins/out20160817_08.csv"), StandardOpenOption.CREATE))) {
			for (String jenkins : jenkisUris) {
				List<String> jobs = new ArrayList<>(getJobsTuples(jenkins));
				Collections.reverse(jobs);

				for (String job : jobs) {
					HashMap<String, Double> indicators = getIndicators(getAllJobs(new Tuple2<>(jenkins, job)));
					if (!printed) {
						out.println("server,job," + indicators.keySet().stream().collect(Collectors.joining(",")) + ";");
						printed = true;
					}

					out.println(toCSVLine(jenkins, job, indicators));
				}
			}
		}
	}
}
