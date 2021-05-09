import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class Grid extends ArrayList<Object> {
    String name;

    public  Grid(String tableName, String[]cols){
       this.init( tableName, cols); //dimension

       this.serialG();
    } //after init size = dimension-1

    public void init(String tableName, String[]cols){
        
    }


















    public void serialG(){
        try
        {
            String dataDirPath = "src/main/resources/data/";
            String filename = dataDirPath+this.name+".ser";


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

}
