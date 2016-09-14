package com.ba.spark.tutorial;

import static com.ba.spark.tutorial.MyOsmUtils.getIndicators;

import com.offbytwo.jenkins.JenkinsServer;
import com.offbytwo.jenkins.model.Build;
import com.offbytwo.jenkins.model.BuildWithDetails;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by bartyushenko on 19.07.16.
 */
public class MultipleJenkinsSparkKM {
	static Logger logger = LogManager.getRootLogger();

	public static void main(String[] strings) throws Exception {

		System.out.println("started ");

		try (JavaSparkContext jsc = new JavaSparkContext(new SparkConf()
				.setJars(MyOsmUtils.JARS)
				.setAppName("20160803_3_1"))) {

			jsc.parallelize(jenkisUris)
					.flatMap(t -> getJobsTuples(t).stream().map(cc -> new Tuple2<>(t, cc)).collect(Collectors.toList()))
					.filter(c -> c._2() != null && !c._2().isEmpty())
					.cache()
					.map(c -> toCSVLine(c._1(), c._2(), getIndicators(getAllJobs(c))))
					.filter(c -> c != null && !c.isEmpty())
					.saveAsTextFile("/work/coe/spark_jenkins/out/" + jsc.appName() + "_" + System.currentTimeMillis());
		}
	}

	static String toCSVLine(String jenkins, String job, HashMap<String, Double> indicators) {
		if (job == null || job.isEmpty()) {
			return "";
		}

		if (indicators == null || indicators.isEmpty()) {
			logger.warn(" no indicators for " + jenkins + " " + job);
			return "";
		}

		return jenkins + "," + job + "," + toCSV(indicators) + ";";
	}

	static HashSet<String> getJobsTuples(String t) {
		try {
			logger.info("getJobsTuples " + t);
			return new HashSet<>(new JenkinsServer(new URI(t), null, null).getJobs().keySet());
		}
		catch (Exception e) {
			logger.error("getJobsTuples " + t, e);
			return new HashSet<String>();
		}
	}
		/**/

	static ArrayList<BuildWithDetails> getAllJobs(Tuple2<String, String> n) {
		logger.info("getAllJobsTuple " + n);
		List<Build> builds = MyOsmUtils.getAllBuildsOfJob(n);
		return
				new ArrayList<>(
						builds.stream()
								.filter(b -> b != null)
								.map(b -> MyOsmUtils.getBuildWithDetails(b))
								.filter(b -> b != null)
								.collect(Collectors.toList()));
	}

	static String toCSV(HashMap<String, Double> indicators) {
		logger.info("toCSV " + indicators + indicators.keySet());
		return indicators.keySet().stream().map(k -> String.valueOf(indicators.get(k))).collect(Collectors.joining(","));
	}

	static final ArrayList<String> jenkisUris = new ArrayList<>(Arrays.asList(
			// taken from https://wiki.jenkins-ci.org/pages/viewpage.action?pageId=58001258 on 2016-07-26

				"https://builds.apache.org/"
				,
				"https://build.kde.org/"
					,
				"https://ci.exoplatform.org/"
				,
					"https://ci.jboss.org/hudson/"
				,

				"https://ci.openquake.org/"
				,

				"https://ci.opensuse.org/"
				,
				"https://jenkins.qa.ubuntu.com/"
				,
				"https://qa.nuxeo.org/jenkins/"
				,
				"https://qa.coreboot.org/"
	));
}
