import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;


public class Page extends Vector<ArrayList<Object>> implements Serializable {

	Vector<Page> overflow;


	

	public Page() {
		overflow= new Vector <>();
	}

	public void addOverflow(Page p){
		overflow.add(p);
	}
	public void removeOverflow(int i){
		overflow.remove(i);
	}
	public boolean isFull(){        //full = false --- can accept entries=true
		boolean res=false;
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream is = null;
		try {
			is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {

		}
		try {
			prop.load(is);
			int max = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
			if (this.size()==max){
				res= true;
			}
			else
				res= false;
		}
		catch (IOException ex) {


		}

		return res;
	}
	public void serialP(String s){
		try
		{
			String filename="src/main/resources/data/"+s+".ser";


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
			FileInputStream fileIn = new FileInputStream("src/main/resources/data/"+s+".ser");
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
