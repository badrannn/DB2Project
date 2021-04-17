import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table  implements Serializable {
    ArrayList<Page> pages= new ArrayList<Page>();

    public Table(){


    }
    public void serialT(){
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream(filename);
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(object);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }
    public void unSerialT(){

    }
    public static void main(string[]args){

    }



}
