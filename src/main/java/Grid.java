import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class Grid extends ArrayList<Object> implements Serializable {
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
    public static String[] sortCols(String tableName, String[]cols){
        String[] x = new String[cols.length];
        String[] colums=Table.returnColumns(tableName);
        ArrayList<Integer> y = new ArrayList <Integer>();
        for (String col:cols) {
            int m = DBApp.coloumnnum(col,tableName);
            y.add(m);
        }
        Collections.sort(y);
        for (int i = 0; i <x.length ; i++) {
            x[i]=colums[y.get(i)];
        }
        return x;
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
//        String[] s = {"name","gpa"};
//        System.out.println(Arrays.toString(Table.returnColumns("trial")));
//        System.out.println(Arrays.toString(sortCols("trial",s)));
    }

}
