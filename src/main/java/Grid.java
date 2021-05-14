import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;


public class Grid implements Serializable {
    String name;
    Object[]grid;

    public  Grid(String tableName, String[]cols) throws DBAppException{
       name=tableName;
       cols=sortCols(tableName,cols);
        for (int i = 0; i <cols.length ; i++) {
            name=name+cols[i];
        }
        String filepath= "src/main/resources/data/"+name+".ser";
        File f = new File(filepath);
        if(f.exists() && !f.isDirectory()){
            throw new DBAppException();
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

    public static String checkBucket(Vector<Integer> bucketnumber, String indexname ){
        Grid k=deserialG(indexname);
        String s =indexname;
        for (int i = 0; i < bucketnumber.size(); i++) {
            s=s+bucketnumber.get(i);
        }
        boolean g=setStuffInArray(k.grid,bucketnumber,0,s);
        if(!g) {
            Bucket b=new Bucket();
            b.serialB(s);
        }
        k.serialG();
        return s;
    }
   public static boolean setStuffInArray(Object[] Grid,Vector<Integer> bucketnumber, int i,Object reference) {
boolean b=true;
     if(i<bucketnumber.size()-1){
         int ii=bucketnumber.get(i);
         Object[] grid2=(Object[])Grid[ii];
         i++;
         b=setStuffInArray(grid2,bucketnumber,i,reference);
     }
     else{
         int ii=bucketnumber.get(i);
         if(Grid[ii]==null) {
             Grid[ii] = reference;
             b=false;
         }
     }
     return b;
   }
    public static boolean bucketempty(Object[] Grid,Vector<Integer> bucketnumber, int i) {

        if(i<bucketnumber.size()-1){
            int ii=bucketnumber.get(i);
            Object[] grid2=(Object[])Grid[ii];
            i++;
            bucketempty(grid2,bucketnumber,i);
        }

            int ii=bucketnumber.get(i);
            if(Grid[ii]==null){
                return true;
            }
            return false;
    }

    public static Grid deserialG(String s){
        Grid p;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/"+s+".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Grid) in.readObject();
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

    public static void main(String[]args) throws DBAppException {
//        String[] s = {"gpa","name"};
//        Grid g = new Grid("trial",s);
//        Vector<Integer> v=new Vector<Integer>();
//        v.add(0);
//        v.add(1);
//        String name="trialgpaname";
//        createbucket(v,name);
     System.out.println(Arrays.deepToString(deserialG("trialgpaname").grid));
     System.out.println(Bucket.deserialB("trialgpaname01"));

    }

}
