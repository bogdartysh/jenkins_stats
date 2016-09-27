package com.ba.spark.tutorial;

import com.offbytwo.jenkins.JenkinsServer;
import com.offbytwo.jenkins.model.Build;
import com.offbytwo.jenkins.model.BuildResult;
import com.offbytwo.jenkins.model.BuildWithDetails;
import com.offbytwo.jenkins.model.JobWithDetails;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;

/**
 * Created by bartyushenko on 23.06.16.
 */
public final class CommontUtils {
	private static Logger log = LogManager.getLogger(CommontUtils.class);

	public static BuildWithDetails getBuildWithDetails(Build b) {
		try {

			return b.details();
		}
		catch (Exception e) {
			log.error(e);
			log.error("failed getBuildWithDetails " + b.getUrl());
			return null;
		}
	}

	public static HashMap<String, Double> getIndicators(List<BuildWithDetails> builds) {
		if (builds == null || builds.isEmpty())
			return new HashMap<>();
		final double[] arr = builds.stream().mapToDouble(t -> t.getDuration() / 1000d).toArray();
		return new HashMap<String, Double>() {
			{
				put("numberOfBuilds", 1d * builds.size());
				put("max2meanDuration", StatUtils.max(arr) / StatUtils.mean(arr));
				// https://en.wikipedia.org/wiki/Index_of_dispersion
				put("vmrDuration", StatUtils.variance(arr) / StatUtils.mean(arr));
				// https://en.wikipedia.org/wiki/Coefficient_of_variation
				put("coefficientOfVariationDuration", Math.sqrt(StatUtils.variance(arr)) / StatUtils
						.mean(arr));
				put("abortsPercentage",
						100d * builds.stream().filter(t -> BuildResult.ABORTED == t.getResult()).count() / (builds.size()));

				put("failuresPercentage",
						100d * builds.stream().filter(t -> BuildResult.FAILURE == t.getResult()).count() / (builds.size()));
				put("successesPercentage",
						100d * builds.stream().filter(t -> BuildResult.SUCCESS == t.getResult()).count() / (builds.size()));
			}
		};
	}

	static List<Build> getAllBuildsOfJob(Tuple2<String, String> jd) {
		try {
			final JenkinsServer server = new JenkinsServer(new URI(jd._1()), null, null);
			JobWithDetails j = server.getJob(jd._2());
			return j.getAllBuilds();
		}
		catch (Exception e) {
			log.error(e);
			log.error("failed getAllBuildsOfJob " + jd._1() + " at" + jd._2());
			System.out.println(e);
			return Collections.<Build>emptyList();
		}
	}

}
