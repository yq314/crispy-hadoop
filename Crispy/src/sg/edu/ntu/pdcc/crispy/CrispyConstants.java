package sg.edu.ntu.pdcc.crispy;

/**
 * This is a constant class which stores the constants defination used in
 * project.
 * 
 * @author qingye
 * 
 */
public final class CrispyConstants {
	public static final float THRESHOLD = 0.10f;
	public static final float EPSILON = 0.00001f;

	public static final int DEFAULT_PROC_NUM = 1;

	public static final String VEC_FILE_EXT = "vec";
	public static final String SEED_FILE_EXT = "seed";
	public static final String INDEX_FILE_EXT = "idx";
	public static final String SEED_NO_VAR_NAME = "crispy.seed.no";
	public static final String READ_NO_VAR_NAME = "crispy.read.no";
	public static final String INPUT_FILE_VAR_NAME = "crispy.input.name";
	public static final String THRESHOLD_VAR_NAME = "crispy.threshold";
	public static final String PROC_NUM_VAR_NAME = "crispy.process.number";

	private CrispyConstants() {
		// this prevents even the native class from calling this constructor as
		// well :
		throw new AssertionError();
	}
}
