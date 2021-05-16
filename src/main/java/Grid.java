import java.io.*;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


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
            populate(tSize);
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
    public void populate(int tablesize){
        ComB comp=new ComB();
        for (int i = 0; i <tablesize ; i++) {
            while(DBApp.checkdeleted(this.tableName,i)){
                i++;
                tablesize++;
            }
            Page p=Page.deserialP(this.tableName+i);
            for (int j = 0; j <p.size() ; j++) {
                ArrayList tuple=new ArrayList();
                tuple=p.get(j);
                ArrayList<Object> inserted=new ArrayList<Object>(this.cols.length);
                for (int k = 0; k < this.cols.length; k++) {
                   int column= DBApp.coloumnnum(this.cols[k],this.tableName);
                   inserted.add(tuple.get(column));
                }
                Vector<Integer> bucketnumber=returnCell(this.name,inserted);
                String buckname=checkBucket(bucketnumber,this.name);
                Bucket buck=Bucket.deserialB(buckname);
                ArrayList bucketinfo=new ArrayList();
                bucketinfo.add(i);
                bucketinfo.add(j);
                bucketinfo.add(false);
                bucketinfo.add(-1);
                bucketinfo.add(tuple.get(0));
                for (int k = 0; k <inserted.size(); k++) {
                    bucketinfo.add(inserted.get(k));
                }
                if(!buck.isFull()) {
                    buck.add(bucketinfo);
                    Collections.sort(buck, comp);
                    buck.serialB(buckname);
                }
                else{
                    boolean flag=false;
                    for (int k = 0; k <buck.overflow.size() ; k++) {
                        if (!buck.overflow.get(k).isFull()){
                            buck.overflow.get(k).add(bucketinfo);
                            Collections.sort( buck.overflow.get(k), comp);
                            buck.serialB(buckname);
                            flag=true;
                            break;
                        }
                    }
                    if(flag==false){
                        Bucket b=new Bucket();
                        b.add(bucketinfo);
                        buck.overflow.add(b);
                        buck.serialB(buckname);
                    }
                }
            }//overflow
            for (int j = 0; j < p.overflow.size(); j++) {
                Page over=p.overflow.get(j);
                for (int k = 0; k < over.size(); k++) {
                    ArrayList tuple=new ArrayList();
                    tuple=over.get(k);
                    ArrayList<Object> inserted=new ArrayList<Object>(this.cols.length);
                    for (int m = 0; m < this.cols.length; m++) {
                        int column= DBApp.coloumnnum(this.cols[m],this.tableName);
                        inserted.add(tuple.get(column));
                    }
                    Vector<Integer> bucketnumber=returnCell(this.name,inserted);
                    String buckname=checkBucket(bucketnumber,this.name);
                    Bucket buck=Bucket.deserialB(buckname);
                    ArrayList bucketinfo=new ArrayList();
                    bucketinfo.add(i);
                    bucketinfo.add(k);
                    bucketinfo.add(true);
                    bucketinfo.add(j);
                    bucketinfo.add(tuple.get(0));
                    for (Object col:inserted) {
                        bucketinfo.add(col);
                    }
                    if(!buck.isFull()) {
                        buck.add(bucketinfo);
                        Collections.sort(buck, comp);
                        buck.serialB(buckname);
                    }
                    else{
                        boolean flag=false;
                        for (int t = 0; t <buck.overflow.size() ; t++) {
                            if (!buck.overflow.get(t).isFull()){
                                buck.overflow.get(t).add(bucketinfo);
                                Collections.sort( buck.overflow.get(t), comp);
                                buck.serialB(buckname);
                                flag=true;
                                break;
                            }
                        }
                        if(flag==false){
                            Bucket b=new Bucket();
                            b.add(bucketinfo);
                            buck.overflow.add(b);
                            buck.serialB(buckname);
                        }
                    }
                }
            }
        }
    }
    public  String checkBucket(Vector<Integer> bucketnumber, String indexname ){

        String s =indexname;
        for (int i = 0; i < bucketnumber.size(); i++) {
            s=s+bucketnumber.get(i);
        }
        boolean g=setStuffInArray(this.grid,bucketnumber,0,s);
        if(!g) {
            Bucket b=new Bucket();
            b.serialB(s);
        }
        this.serialG();
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

    public Vector<Integer> returnCell(String gridName, ArrayList<Object> inserted){
        Vector<Integer> ret = new Vector<Integer>();
        for (int i = 0; i <inserted.size() ; i++) {
            Object ins = inserted.get(i);
            if(ins==null){
                ret.add(0);
            }
            else if (ins instanceof Integer){
                int j = integCell(this.cols[i],this.tableName,(int)inserted.get(i)) ;
                ret.add(j);
            }
            else if (ins instanceof Double){
                int j = doubleCell(this.cols[i],this.tableName,(Double) inserted.get(i)) ;
                ret.add(j);
            }
            else if(ins instanceof String){
                int j = StringCell(this.cols[i],this.tableName,(String) inserted.get(i)) ;
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

    public static int dateCell(String colName, String tableName, Date inserted) throws ParseException {
        String[] minMax = DBApp.returnMinMax(tableName,colName);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date min = format.parse(minMax[0]);
        Date max = format.parse(minMax[1]);
        int ymin=min.getYear();
        int ymax=max.getYear();
        int mmin=min.getMonth();
        int mmax=max.getMonth();
        int dmin=min.getDay();
        int dmax=min.getDate();

        int small=(ymin*10000)+(mmin*100)+dmin;
        int big=(ymax*10000)+(mmax*100)+dmax;
        int range=big-small;
        int div=range/10;

        int a=inserted.getYear();
        int b=inserted.getMonth();
        int c=inserted.getDay();
        int d=(a*10000)+(b*100)+c;

        for (int i = 0; i < 10 ; i++) {
            if(d<(small+(div*i)))
                return i;
        }

        return 11; //means sth is wrong with the method.
    }

    public static void main(String[]args) throws DBAppException, ParseException {
        //String[] s = {"id","name"};
//        Vector<Integer> v=new Vector<Integer>();
//        v.add(0);
//        v.add(1);
//        String name="trialgpaname";
//        createbucket(v,name);
        //Object[] o ={3.9,"AAAA"};
//        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
  //      String sDate2 = "31-12-1998";
   //     Date min = format.parse(sDate2);



     //   Table s=new Table("d");

     // System.out.print(dateCell("a", "d",min));
        //DBApp db = new DBApp();
        //db.init();
/*
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Hashtable htblColNameMin = new Hashtable();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", " ");
        htblColNameMin.put("gpa", "0");
        Hashtable htblColNameMax = new Hashtable();
        htblColNameMax.put("id", "213981");
        htblColNameMax.put("name", "ZZZZZZZZZZ");
        htblColNameMax.put("gpa", "5");
         */
        //Hashtable<String, String> htblColNameType = null;
        //Hashtable<String, String> htblColNameMin = null;
        //Hashtable<String, String> htblColNameMax=null;
        //db.createTable("trial", "id", htblColNameType);


    }

}
