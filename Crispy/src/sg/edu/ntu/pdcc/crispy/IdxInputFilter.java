package sg.edu.ntu.pdcc.crispy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * This filter filters the file without .vec out.
 * 
 * @author qingye
 * 
 */
public class IdxInputFilter extends Configured implements PathFilter {
	FileSystem fileSystem;

	@Override
	public boolean accept(Path path) {
		try {
			if (fileSystem.isFile(path)) {
				String fileName = path.getName();
				String[] extName = fileName.split("\\.");
				return CrispyConstants.INDEX_FILE_EXT
						.equalsIgnoreCase(extName[extName.length - 1]) ? true
						: false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public void setConf(Configuration conf) {
		if (conf != null) {
			try {
				fileSystem = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
