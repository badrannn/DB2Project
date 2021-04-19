import java.io.*;
import java.util.Vector;


public class Table extends Vector<Page> implements Serializable {

     String   name;

    public Table(String name){
        this.name=name;

    }
    public void serialT(){
        try
        {
            String filename = name+".ser";


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

    public void serialP(){
        for(int i =0; i<this.size();i++)
        this.get(i).serialP(i+this.name);
    }


    public static void main(String[]args) {

    }
}
