import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;


public class Grid implements Serializable {

  String name;
  Object[] grid;
  String[] cols;
  String tableName;

  public Grid(String tableName, String[] cols) throws DBAppException {
    this.tableName = tableName;
    name = tableName;
    cols = sortCols(tableName, cols);
    this.cols = cols;
    for (int i = 0; i < cols.length; i++) {
      name = name + cols[i];
    }
    String filepath = "src/main/resources/data/" + name + ".ser";
    File f = new File(filepath);
    if (f.exists() && !f.isDirectory()) {
      throw new DBAppException();
    }
    final int[] dimensions = new int[cols.length];
    Arrays.fill(dimensions, 10);
    grid = (Object[]) Array.newInstance(Object.class, dimensions);

    int tSize = Table.deserialT(tableName);
    if (tSize != 0) {
      populate(tSize);
    }

    this.serialG();
  }

  public static String[] sortCols(String tableName, String[] cols) {
    String[] x = new String[cols.length];
    String[] colums = Table.returnColumns(tableName);
    ArrayList<Integer> y = new ArrayList<Integer>();
    for (String col : cols) {
      int m = DBApp.coloumnnum(col, tableName);
      y.add(m);
    }
    Collections.sort(y);
    for (int i = 0; i < x.length; i++) {
      x[i] = colums[y.get(i)];
    }
    return x;
  }

  public void populate(int tablesize) {// row structure [page number,tuple number,overflow or no,page number in overflow, primary key, info
    ComB comp = new ComB();
    for (int i = 0; i < tablesize; i++) {
      while (DBApp.checkdeleted(this.tableName, i)) {
        i++;
        tablesize++;
      }
      Page p = Page.deserialP(this.tableName + i);
      for (int j = 0; j < p.size(); j++) {
        ArrayList tuple = new ArrayList();
        tuple = p.get(j);
        ArrayList<Object> inserted = new ArrayList<Object>(this.cols.length);
        for (int k = 0; k < this.cols.length; k++) {
          int column = DBApp.coloumnnum(this.cols[k], this.tableName);
          inserted.add(tuple.get(column));
        }
        Vector<Integer> bucketnumber = returnCell(this.name, inserted);
        String buckname = checkBucket(bucketnumber, this.name);
        Bucket buck = Bucket.deserialB(buckname);
        ArrayList bucketinfo = new ArrayList();
        bucketinfo.add(i);
        bucketinfo.add(j);
        bucketinfo.add(false);
        bucketinfo.add(-1);
        bucketinfo.add(tuple.get(0));
        for (int k = 0; k < inserted.size(); k++) {
          bucketinfo.add(inserted.get(k));
        }
        if (!buck.isFull()) {
          buck.add(bucketinfo);
          Collections.sort(buck, comp);
          buck.serialB(buckname);
        } else {
          boolean flag = false;
          for (int k = 0; k < buck.overflow.size(); k++) {
            if (!buck.overflow.get(k).isFull()) {
              buck.overflow.get(k).add(bucketinfo);
              Collections.sort(buck.overflow.get(k), comp);
              buck.serialB(buckname);
              flag = true;
              break;
            }
          }
          if (flag == false) {
            Bucket b = new Bucket();
            b.add(bucketinfo);
            buck.overflow.add(b);
            buck.serialB(buckname);
          }
        }
      }//overflow
      for (int j = 0; j < p.overflow.size(); j++) {
        Page over = p.overflow.get(j);
        for (int k = 0; k < over.size(); k++) {
          ArrayList tuple = new ArrayList();
          tuple = over.get(k);
          ArrayList<Object> inserted = new ArrayList<Object>(this.cols.length);
          for (int m = 0; m < this.cols.length; m++) {
            int column = DBApp.coloumnnum(this.cols[m], this.tableName);
            inserted.add(tuple.get(column));
          }
          Vector<Integer> bucketnumber = returnCell(this.name, inserted);
          String buckname = checkBucket(bucketnumber, this.name);
          Bucket buck = Bucket.deserialB(buckname);
          ArrayList bucketinfo = new ArrayList();
          bucketinfo.add(i);
          bucketinfo.add(k);
          bucketinfo.add(true);
          bucketinfo.add(j);
          bucketinfo.add(tuple.get(0));
          for (Object col : inserted) {
            bucketinfo.add(col);
          }
          if (!buck.isFull()) {
            buck.add(bucketinfo);
            Collections.sort(buck, comp);
            buck.serialB(buckname);
          } else {
            boolean flag = false;
            for (int t = 0; t < buck.overflow.size(); t++) {
              if (!buck.overflow.get(t).isFull()) {
                buck.overflow.get(t).add(bucketinfo);
                Collections.sort(buck.overflow.get(t), comp);
                buck.serialB(buckname);
                flag = true;
                break;
              }
            }
            if (flag == false) {
              Bucket b = new Bucket();
              b.add(bucketinfo);
              buck.overflow.add(b);
              buck.serialB(buckname);
            }
          }
        }
      }
    }
  }

  public String checkBucket(Vector<Integer> bucketnumber, String indexname) {

    String s = indexname;
    for (int i = 0; i < bucketnumber.size(); i++) {
      s = s + bucketnumber.get(i);
    }
    boolean g = setStuffInArray(this.grid, bucketnumber, 0, s);
    if (!g) {
      Bucket b = new Bucket();
      b.serialB(s);
    }
    this.serialG();
    return s;
  }

  public static boolean setStuffInArray(Object[] Grid, Vector<Integer> bucketnumber, int i,
      Object reference) {
    boolean b = true;
    if (i < bucketnumber.size() - 1) {
      int ii = bucketnumber.get(i);
      Object[] grid2 = (Object[]) Grid[ii];
      i++;
      b = setStuffInArray(grid2, bucketnumber, i, reference);
    } else {
      int ii = bucketnumber.get(i);
      if (Grid[ii] == null) {
        Grid[ii] = reference;
        b = false;
      }
    }
    return b;
  }

  public static boolean bucketempty(Object[] Grid, Vector<Integer> bucketnumber, int i) {

    if (i < bucketnumber.size() - 1) {
      int ii = bucketnumber.get(i);
      Object[] grid2 = (Object[]) Grid[ii];
      i++;
      bucketempty(grid2, bucketnumber, i);
    }

    int ii = bucketnumber.get(i);
    if (Grid[ii] == null) {
      return true;
    }
    return false;
  }
  public static Bucket returnbuck(Object[] Grid, Vector<Integer> bucketnumber, int i){
    if (i < bucketnumber.size() - 1) {
      int ii = bucketnumber.get(i);
      Object[] grid2 = (Object[]) Grid[ii];
      i++;
      bucketempty(grid2, bucketnumber, i);
    }

    int ii = bucketnumber.get(i);
    String ff =(String)Grid[ii];
    String filePathString ="src/main/resources/data/"+ff+".ser";
    File f = new File(filePathString);
    if (f.exists() && !f.isDirectory()) {
      Bucket b = Bucket.deserialB((String) Grid[ii]);
      return b;
    }
    else return null;
  }

  public static Grid deserialG(String s) {
    Grid p;
    try {
      FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + s + ".ser");
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

  public void serialG() {
    try {
      String dataDirPath = "src/main/resources/data/";
      String filename = dataDirPath + this.name + ".ser";

      FileOutputStream file = new FileOutputStream(filename);
      ObjectOutputStream out = new ObjectOutputStream(file);

      out.writeObject(this);

      out.close();
      file.close();

      System.out.println("Object has been serialized");

    } catch (IOException ex) {
      System.out.println("IOException is caught");
    }

  }

  public Vector<Integer> returnCell(String gridName, ArrayList<Object> inserted) {
    Vector<Integer> ret = new Vector<Integer>();
    for (int i = 0; i < inserted.size(); i++) {
      Object ins = inserted.get(i);
      if (ins == null) {
        ret.add(0);
      } else if (ins instanceof Integer) {
        int j = integCell(this.cols[i], this.tableName, (int) inserted.get(i));
        ret.add(j);
      } else if (ins instanceof Double) {
        int j = doubleCell(this.cols[i], this.tableName, (Double) inserted.get(i));
        ret.add(j);
      } else if (ins instanceof String) {
        int j = StringCell(this.cols[i], this.tableName, (String) inserted.get(i));
        ret.add(j);

      } else {

      }

    }

    return ret;
  }

  public static int StringCell(String colName, String tableName, String inserted) {
    String[] minMax = DBApp.returnMinMax(tableName, colName);
    int min = 0;
    int max = 0;
    int valu = 0;
    for (int i = 0; i < minMax[0].length(); i++) {
      min = min + (int) minMax[0].charAt(i);
    }
    for (int i = 0; i < minMax[1].length(); i++) {
      max = max + (int) minMax[1].charAt(i);
    }
    for (int i = 0; i < inserted.length(); i++) {
      valu = valu + (int) inserted.charAt(i);
    }
    int diff = max - min;
    for (int i = 0; i < 10; i++) {
      if (valu <= (min) + (((i + 1) * (diff / 10)))) {
        return i;
      }
    }
    return 9;

  }
  public static void updatebucket(){
    
  }

  public static int doubleCell(String colName, String tableName, Double inserted) {
    String[] minMax = DBApp.returnMinMax(tableName, colName);
    double diff = Double.parseDouble(minMax[1]) - Double.parseDouble(minMax[0]);

    for (int i = 0; i < 10; i++) {
      if (inserted <= (Double.parseDouble(minMax[0])) + (((i + 1) * (diff / 10.0d)))) {
        return i;
      }
    }
    return 9;

  }

  public static int integCell(String colName, String tableName, int inserted) {
    String[] minMax = DBApp.returnMinMax(tableName, colName);
    int diff = Integer.parseInt(minMax[1]) - Integer.parseInt(minMax[0]);

    for (int i = 0; i < 10; i++) {
      if (inserted <= (Integer.parseInt(minMax[0])) + (((i + 1) * (diff / 10)))) {
        return i;
      }
    }
    return 9;


  }
  public static void insertIntoBucket(String gridName,  ArrayList<Object> key,ArrayList<Object> bucketinfo){
    ComB comp =new ComB();
    Grid g = deserialG(gridName);
    Vector<Integer> v = g.returnCell(gridName,key);
    String buckname = g.checkBucket(v,g.name);
    Bucket buck = Bucket.deserialB(buckname);
    if(!buck.isFull()){
      buck.add(bucketinfo);
      Collections.sort(buck, comp);
      buck.serialB(buckname);
    }
    else {
      boolean flag = false;
      for (int k = 0; k < buck.overflow.size(); k++) {
        if (!buck.overflow.get(k).isFull()) {
          buck.overflow.get(k).add(bucketinfo);
          Collections.sort(buck.overflow.get(k), comp);
          buck.serialB(buckname);
          flag = true;
          break;
        }
      }
      if (!flag) {
        Bucket b = new Bucket();
        b.add(bucketinfo);
        buck.overflow.add(b);
        buck.serialB(buckname);
      }
    }




  }

  public static void main(String[] args) throws DBAppException {
    //String[] s = {"id","name"};
//        Vector<Integer> v=new Vector<Integer>();
//        v.add(0);
//        v.add(1);
//        String name="trialgpaname";
//        createbucket(v,name);
    Object[] o = {3.9, "AAAA"};

  }

}
