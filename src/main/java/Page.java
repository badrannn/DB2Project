import java.io.Serializable;
import java.util.Vector;

/**
 * 
 */

/**
 * @author peter
 *
 */
public class Page extends Vector<Object> implements Serializable {

	int max = 200;
	int tuple;// Number of tuples
	

	public Page() {

	}

	public boolean addTuple() {
		if (tuple < max - 1) {
			tuple++;
			return true;
		} else
			return false;

	}

	public boolean removeTuple() {
		if (tuple == 1) {
			tuple--;
			return true;// must delete the page afterward
		}
		if (tuple > 1) {
			tuple--;
			return true;
		} else
			return false;

	}

	public boolean isFull() {
		if (tuple == max)
			return true;
		else
			return false;
	}

	public boolean isEmpty() {
		if (tuple == 0)
			return true;
		else
			return false;
	}
	/**
	 * @param args
	 */

}
