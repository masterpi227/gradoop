package org.gradoop.examples.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FrequentSubgraphMiningBenchmark extends AbstractRunner implements ProgramDescription {


  /**
   * Option to declare path to input graph
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Path to CSV log file
   */
  public static final String OPTION_CSV_PATH = "csv";
  /**
   * Used hdfs inputPath
   */
  private static String inputPath;
  /**
   * Used hdfs outputPath
   */
  private static String outputPath;
  /**
   * Used csv path
   */
  private static String csvPath;

  static{
    OPTIONS.addOption(OPTION_INPUT_PATH, "vertex-input-path", true,
      "Path to vertex file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Path to write output files to");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv-path", true, "Path of the " +
      "generated CSV-File");
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, FrequentSubgraphMiningBenchmark.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    csvPath = cmd.getOptionValue(OPTION_CSV_PATH);

    // initialize EPGM database
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graphDatabase =
      readLogicalGraph(inputPath, false);


  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File needed");
    }
  }

  @Override
  public String getDescription() {
    return FrequentSubgraphMiningBenchmark.class.getName();
  }
}
