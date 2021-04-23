import org.javatuples.Triplet;

import java.io.*;
import java.util.Vector;


public class Table extends Vector<Page> implements Serializable {

     String   name;
     String cluster;
     String[] columns;

    public Table(String name){
        this.name=name;

    }
    public void serialT(){
        try
        {
            String dataDirPath = "src/main/resources/data/";
            String filename = dataDirPath+name+".ser";


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
        this.get(i).serialP(this.name+i);
    }
    public static int deserialT(String s){ //return size
        Table t ;
        try {
            FileInputStream fileIn = new FileInputStream( "src/main/resources/data/"+s+".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
            return t.size();
        } catch (IOException i) {
            i.printStackTrace();
            return -1;
        } catch (ClassNotFoundException c) {
            System.out.println(" class not found");
            c.printStackTrace();
            return -1;
        }
    }
    public static String returnCluster(String s){
        Table t ;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/"+s+".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
            return t.cluster;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println(" class not found");
            c.printStackTrace();
            return null;
        }
    }
    public static String[] returnColumns(String s){
        Table t ;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/"+s+".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
            return t.columns;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println(" class not found");
            c.printStackTrace();
            return null;
        }

    }


    public static void main(String[]args) {

    }
}
