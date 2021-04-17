import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table  implements Serializable {
    ArrayList<Page> pages;

    public Table(){
        pages= new ArrayList<Page>();


    }
    public void serialT(){
        try
        {
            String filename = "file.ser";
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream(filename);
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
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
    public void unSerialT(){

    }
    public static void main(String[]args){
        Table t = new Table();
        t.serialT();


    }



}
