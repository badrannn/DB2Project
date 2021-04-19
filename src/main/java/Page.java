import java.io.*;
import java.math.BigInteger;
import java.util.Vector;


public class Page extends Vector<Object> implements Serializable {

	int max = 200;
	int tuple;// Number of tuples
	

	public Page() {
	tuple=0;

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
	public void serialP(String s){
		try
		{
			String filename=s+".ser";


			FileOutputStream file = new FileOutputStream(filename);
			ObjectOutputStream out = new ObjectOutputStream(file);


			out.writeObject(this);

			out.close();
			file.close();

			System.out.println("Object has been serialized");

		}
		catch(IOException ex)
		{
			System.out.println("IOException is caught");
		}

	}

	public static Page  deserialP(String s){
		Page p;
		try {
			FileInputStream fileIn = new FileInputStream(s+".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Page) in.readObject();
			in.close();
			fileIn.close();
			return p;
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException c) {
			System.out.println(" class not found");
			c.printStackTrace();
			return null;
		}
	}



}
