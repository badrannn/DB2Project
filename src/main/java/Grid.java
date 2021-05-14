import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;


public class Grid implements Serializable {
    String name;
    Object[]grid;
    String [] cols;
    String tableName;
    public  Grid(String tableName, String[]cols) throws DBAppException{
       this.tableName=tableName;
        name=tableName;
       cols=sortCols(tableName,cols);
       this.cols=cols;
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

    public static Vector<Integer> returnCell(String gridName, Object[] inserted){
        Vector<Integer> ret = new Vector<Integer>();
        Grid g = deserialG(gridName);
        for (int i = 0; i <inserted.length ; i++) {
            Object ins = inserted[i];
            if (ins instanceof Integer){
                int j = integCell(g.cols[i],g.tableName,(int)inserted[i]) ;
                ret.add(j);
            }
            else if (ins instanceof Double){
                int j = doubleCell(g.cols[i],g.tableName,(Double) inserted[i]) ;
                ret.add(j);
            }
            else if(ins instanceof String){
                int j = StringCell(g.cols[i],g.tableName,(String) inserted[i]) ;
                ret.add(j);

            }
            else {

            }

        }


        return ret;
    }
    public static int StringCell(String colName,String tableName,String inserted){
        String[] minMax = DBApp.returnMinMax(tableName,colName);
        int min = 0;
        int max=0;
        int valu=0;
        for (int i = 0; i <minMax[0].length() ; i++) {
            min= min+ (int)minMax[0].charAt(i);
        }
        for (int i = 0; i <minMax[1].length() ; i++) {
            max= max+ (int)minMax[1].charAt(i);
        }
        for (int i = 0; i <inserted.length() ; i++) {
            valu= valu+ (int)inserted.charAt(i);
        }
        int diff = max-min;
        for (int i = 0; i <10 ; i++) {
            if (valu <= (min) + (((i + 1) * (diff / 10)))){
                return i;
            }
        }
        return 9;

    }
    public static int doubleCell(String colName,String tableName,Double inserted){
        String[] minMax = DBApp.returnMinMax(tableName,colName);
        double diff = Double.parseDouble(minMax[1])-Double.parseDouble(minMax[0]);

        for (int i = 0; i <10 ; i++) {
            if (inserted <= (Double.parseDouble(minMax[0])) + (((i + 1) * (diff / 10.0d)))){
                return i;
            }
        }
        return 9;

    }
    public static int  integCell(String colName,String tableName, int inserted){
        String[] minMax = DBApp.returnMinMax(tableName,colName);
        int diff = Integer.parseInt(minMax[1])-Integer.parseInt(minMax[0]);

            for (int i = 0; i <10 ; i++) {
                if (inserted <= (Integer.parseInt(minMax[0])) + (((i + 1) * (diff / 10)))){
                    return i;
                }
            }
                    return 9;



    }

    public static void main(String[]args) throws DBAppException {
        //String[] s = {"id","name"};
//        Vector<Integer> v=new Vector<Integer>();
//        v.add(0);
//        v.add(1);
//        String name="trialgpaname";
//        createbucket(v,name);
        Object[] o ={3.9,"AAAA"};
        System.out.println(returnCell("trialgpaname",o));

    }

}
