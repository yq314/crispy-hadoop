package sg.edu.ntu.pdcc.crispy;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CrispyUtil {

	public static int countFileLines(Configuration conf, String fileName)
			throws IOException {
		FileSystem fs = FileSystem.get(URI.create(fileName), conf);
		FSDataInputStream is = fs.open(new Path(fileName));
		BufferedInputStream bis = new BufferedInputStream(is);

		byte[] c = new byte[1024];
		int count = 0;
		int readChars = 0;

		while ((readChars = bis.read(c)) != -1) {
			for (int i = 0; i < readChars; i++) {
				if (c[i] == '\n') {
					++count;
				}
			}

		}

		bis.close();
		return count;
	}
}
