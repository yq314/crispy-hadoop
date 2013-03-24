package sg.edu.ntu.pdcc.crispy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EuclidDistMapper extends
		Mapper<LongWritable, Text, FloatWritable, Text> {
	private Text outKey = new Text();
	private FloatWritable outVal = new FloatWritable(0);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int seedNum = conf.getInt(CrispyConstants.SEED_NO_VAR_NAME, 0);
		long readNum = conf.getLong(CrispyConstants.READ_NO_VAR_NAME, 0);
		int procNum = conf.getInt(CrispyConstants.PROC_NUM_VAR_NAME, 0);

		String inputFileName = conf.get(CrispyConstants.INPUT_FILE_VAR_NAME);

		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(new Path(inputFileName + "."
						+ CrispyConstants.VEC_FILE_EXT))));

		String val = value.toString();
		String[] indexes = val.split(",");
		long baseSize = readNum / procNum;
		long extra = readNum % procNum;
		long startX = Long.parseLong(indexes[0]);
		long blockSizeX = (startX - 1) / (baseSize + 1) < extra ? (baseSize + 1)
				: baseSize;
		long startIndexX = (startX - 1) * seedNum + 1;
		long endIndexX = startIndexX + blockSizeX * seedNum;

		long startY = Long.parseLong(indexes[1]);
		long blockSizeY = (startY - 1) / (baseSize + 1) < extra ? (baseSize + 1)
				: baseSize;
		long startIndexY = (startY - 1) * seedNum + 1;
		long endIndexY = startIndexY + blockSizeY * seedNum;

		float[][] dataX = new float[(int) blockSizeX][seedNum];
		float[][] dataY = new float[(int) blockSizeY][seedNum];
		long[] indexArrayX = new long[(int) blockSizeX];
		long[] indexArrayY = new long[(int) blockSizeY];

		String line = null;
		long count = 1;
		int indexX = 0, indexY = 0;
		int indexSeedX = 0, indexSeedY = 0;
		while ((line = br.readLine()) != null) {
			if (count >= startIndexX && count < endIndexX) {
				dataX[indexX][indexSeedX++] = Float.parseFloat(line);
				if (indexSeedX == seedNum) {
					indexArrayX[indexX++] = count / seedNum;
					indexSeedX = 0;
				}
			}
			if (count >= startIndexY && count < endIndexY) {
				dataY[indexY][indexSeedY++] = Float.parseFloat(line);
				if (indexSeedY == seedNum) {
					indexArrayY[indexY++] = count / seedNum;
					indexSeedY = 0;
				}
			}
			++count;
		}

		// Compute the euclid distance
		float dist, diff, sum;
		float threshold = conf.getFloat(CrispyConstants.THRESHOLD_VAR_NAME, 0);
		if (threshold == 0) {
			threshold = CrispyConstants.THRESHOLD;
		}

		int offset = (indexArrayX[0] >= indexArrayY[0]) ? 1 : 0;
		for (int i = 0; i < indexArrayX.length; i++) {
			for (int j = i + offset; j < indexArrayY.length; j++) {
				sum = 0;
				for (int k = 0; k < seedNum; k++) {
					diff = dataX[i][k] - dataY[j][k];
					sum += diff * diff;
				}

				dist = (float) Math.sqrt(sum / seedNum);
				if (dist < threshold
						|| Math.abs(dist - threshold) < CrispyConstants.EPSILON) {
					outKey.set(indexArrayX[i] + "," + indexArrayY[j]);
					outVal.set(dist);
					// context.write(outKey, outVal);
					context.write(outVal, outKey);
				}
			}
		}

	}
}
