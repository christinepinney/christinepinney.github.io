
/**
 * Indicates invalid command line arguments. 
 * @author cpinney
 */

@SuppressWarnings("serial")
public class InvalidCommandLineArgsException extends RuntimeException {

	public InvalidCommandLineArgsException(String msg) {
		super(msg);
	}

}
