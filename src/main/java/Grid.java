import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class Grid extends ArrayList<Object> {
    String name;
    Object[]grid;

    public  Grid(String tableName, String[]cols){
       name=tableName;
        for (int i = 0; i <cols.length ; i++) {
            name=name+cols[i];
        }
        final int[] dimensions = new int[cols.length];
        Arrays.fill(dimensions, 10);
        grid = (Object[]) Array.newInstance(Object.class, dimensions);

        int tSize =Table.deserialT(tableName);
        if(tSize!=0){
            
        }


        this.serialG();
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

    public static void main(String[]args){
        String[] s = {"gpa","name"};
        Grid g = new Grid("students",s);
        System.out.println(Arrays.deepToString(g.grid));

    }

}
