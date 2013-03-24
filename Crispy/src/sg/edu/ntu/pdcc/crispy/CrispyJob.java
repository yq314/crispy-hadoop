package sg.edu.ntu.pdcc.crispy;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CrispyJob extends Configured implements Tool {

	public int printUsage() {
		System.err.printf(
				"Usage: %s [-p procNum] [-t threshold] <input> <output>\n",
				getClass().getSimpleName());
		ToolRunner.printGenericCommandUsage(System.err);
		return -1;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		long elapse = -System.currentTimeMillis();

		int exitCode = ToolRunner.run(new CrispyJob(), args);

		elapse += System.currentTimeMillis();
		System.out.println(">>>>>Time eplased for job: " + elapse / 1.e3 + "s");

		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// set default parameters
		conf.setFloat(CrispyConstants.THRESHOLD_VAR_NAME,
				CrispyConstants.THRESHOLD);

		// ===================================================
		// Parsing the user defined parameters
		// ===================================================
		int procNum = CrispyConstants.DEFAULT_PROC_NUM;

		List<String> argList = new ArrayList<String>();
		for (int i = 0; i < otherArgs.length; i++) {
			if ("-p".equals(args[i])) {
				// set the process number, it defines how many reads are
				// distributed to a semi block
				procNum = Integer.parseInt(args[++i]);
				if (procNum == 0) {
					System.err
							.println("Process number should be larger than 0, exit program.");
					return printUsage();
				}
			} else if ("-t".equals(args[i])) {
				// set the threshold of euclid distance
				float threshold = Float.parseFloat(args[++i]);
				conf.setFloat(CrispyConstants.THRESHOLD_VAR_NAME, threshold);
			} else if (!getClass().getSimpleName().equals(args[i])) {
				// input and output directories send to hadoop
				argList.add(args[i]);
			}
		}
		conf.setInt(CrispyConstants.PROC_NUM_VAR_NAME, procNum);

		// must include input and output directories
		if (argList.size() != 2) {
			System.err.println("Please check your arguments for application.");
			return printUsage();
		}

		String inputFileName = argList.get(0);
		conf.set(CrispyConstants.INPUT_FILE_VAR_NAME, inputFileName);
		String outputDir = argList.get(1);
		Path outputPath = new Path(outputDir);

		// delete the output dir before job running
		FileSystem fs = FileSystem.get(URI.create(outputDir), conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		// ===================================================
		// Preprocessing, create an index file
		// ===================================================

		// read seeds number and reads number from input
		int seedNum = CrispyUtil.countFileLines(conf, inputFileName + "."
				+ CrispyConstants.SEED_FILE_EXT) - 1;
		conf.setInt(CrispyConstants.SEED_NO_VAR_NAME, seedNum);
		long readNum = CrispyUtil.countFileLines(conf, inputFileName + "."
				+ CrispyConstants.VEC_FILE_EXT)
				/ seedNum;
		conf.setLong(CrispyConstants.READ_NO_VAR_NAME, readNum);

		// assign the read index into blocks
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				fs.create(new Path(inputFileName + "."
						+ CrispyConstants.INDEX_FILE_EXT))));

		long writeFileTime = -System.currentTimeMillis();

		// assume the read index start from 1
		long baseSize = readNum / procNum;
		long extra = readNum % procNum;
		long counter, counter2 = extra;
		for (long i = 1; i <= readNum; i += baseSize) {
			counter = extra;
			for (long j = 1; j <= readNum; j += baseSize) {
				out.write(i + "," + j + "\n");

				if (counter > 0) {
					--counter;
					++j;
				}
			}
			if (counter2 > 0) {
				--counter2;
				++i;
			}
		}

		writeFileTime += System.currentTimeMillis();
		System.out.println(">>>>>Time elapsed for writing: " + writeFileTime
				/ 1.e3 + "s");

		out.close();

		// ===================================================
		// Initializing the hadoop job
		// ===================================================
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName("euclidDist");

		job.setInputFormatClass(NLineInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputFileName).getParent());
		FileInputFormat.setInputPathFilter(job, IdxInputFilter.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		NLineInputFormat.setNumLinesPerSplit(job, 1);

		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(EuclidDistMapper.class);
		job.setReducerClass(SortingReducer.class);
		job.setNumReduceTasks(1); // Only 1 reducer ensures only 1 output file

		long processTime = -System.currentTimeMillis();
		boolean state = job.waitForCompletion(false);
		processTime += System.currentTimeMillis();
		System.out.println(">>>>>Time elapsed for processing: " + processTime
				/ 1.e3 + "s");

		return state ? 1 : 0;
	}
}
