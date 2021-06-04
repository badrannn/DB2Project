import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Scanner;
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
    addIndex(this.name);
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

  public void populate(
      int
          tablesize) { // row structure [0page number, 1primary key, 2info
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
      } // overflow
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

  public static boolean setStuffInArray(
      Object[] Grid, Vector<Integer> bucketnumber, int i, Object reference) {
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


  public static Bucket returnbuck(Object[] Grid, Vector<Integer> bucketnumber, int i) {
    if (i < bucketnumber.size() - 1) {
      int ii = bucketnumber.get(i);
      Object[] grid2 = (Object[]) Grid[ii];
      i++;
      bucketempty(grid2, bucketnumber, i);
    }

    int ii = bucketnumber.get(i);
    String ff = (String) Grid[ii];
    String filePathString = "src/main/resources/data/" + ff + ".ser";
    File f = new File(filePathString);
    if (f.exists() && !f.isDirectory()) {
      Bucket b = Bucket.deserialB((String) Grid[ii]);
      return b;
    } else return null;
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

  public static void updatebucket() {}

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
  public static void deletefrombucket(Object key,String buckname ){
    Bucket buck=Bucket.deserialB(buckname);
   int tupl= buck.binarysearch(key);
   int overflowbuck=-1;
    if (tupl == -1) {
      for (int i = 0; i < buck.overflow.size(); i++) {
        Bucket overbuck = buck.overflow.get(i);
        tupl = overbuck.binarysearch(key);
        if (tupl != -1) {
          buck.overflow.get(i).removeElementAt(tupl);

          break;
        }
      }
    } else {
      buck.removeElementAt(tupl);
    }
    buck.serialB(buckname);
  }
  public static void deletefromindex(String tablename,ArrayList<Object> tuple) {
    ArrayList<String> indexes=returnindex(tablename);
    for (int i = 0; i < indexes.size(); i++) {
      Grid g=Grid.deserialG(indexes.get(i));
      ArrayList<Object> key=new ArrayList<Object>();
      String[] columns=g.cols;
      for (int j = 0; j <columns.length ; j++) {
        int col= DBApp.coloumnnum(columns[j],tablename);
        key.add(tuple.get(col));
      }
      Vector<Integer> v = g.returnCell(g.name,key);
      String s = indexes.get(i);
      for (int k = 0; k < v.size(); k++) {
        s = s + v.get(k);
      }
      deletefrombucket(tuple.get(0),s);

    }
  }

  public static void insertIntoBucket(
      Grid g, ArrayList<Object> key, ArrayList<Object> bucketinfo) {
    ComB comp = new ComB();
    Vector<Integer> v = g.returnCell(g.name, key);
    String buckname = g.checkBucket(v, g.name);
    Bucket buck = Bucket.deserialB(buckname);
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
      if (!flag) {
        Bucket b = new Bucket();
        b.add(bucketinfo);
        buck.overflow.add(b);
        buck.serialB(buckname);
      }
    }
  }
  public static void insertupletoindex(String tablename,ArrayList<Object> tuple,Integer pagenum){
    ArrayList<String> indexes=returnindex(tablename);
    for (int i = 0; i <indexes.size() ; i++) {
      ArrayList<Object> bucketinfo=new ArrayList<Object>();
      ArrayList<Object> key=new ArrayList<Object>();
      bucketinfo.add(pagenum);
      bucketinfo.add(tuple.get(0));
      Grid g=deserialG(indexes.get(i));
      String[] columns=g.cols;
      for (int j = 0; j <columns.length ; j++) {
       int col= DBApp.coloumnnum(columns[j],tablename);
       bucketinfo.add(tuple.get(col));
       key.add(tuple.get(col));
      }
      insertIntoBucket(g,key,bucketinfo);
    }
  }
  public static ArrayList<String> returnindex(String name){
    ArrayList<String> result = new ArrayList<>();
    try {
      Scanner scanner = new Scanner(new File("src/main/resources/data/Index.txt"));
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if(line.toLowerCase().contains(name.toLowerCase()))
          result.add(line);

      }
      scanner.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    return result;
  }
  public static void addIndex(String name){

    try {
      //removeBlanksIndex();
      FileWriter pww = new FileWriter("src/main/resources/data/Index.txt", true);
      StringBuilder builder = new StringBuilder();
      builder.append(name+"\n");
      pww.append(builder.toString());
      pww.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    }


  public static void removeBlanksIndex() {
    Scanner file;
    PrintWriter writer;
    String path = "src/main/resources/data/Index.txt";
    try {
      file = new Scanner(new File("src/main/resources/data/range.txt")); // sourcefile
      writer = new PrintWriter("temppp.txt"); // destinationfile
      while (file.hasNext()) {
        String line = file.nextLine();
        if (!line.isEmpty()) {
          writer.write(line);
          writer.write("\n");
        }
      }
      file.close();
      writer.close();
      Files.delete(Paths.get(path));
      File newfile = new File("temppp.txt");
      File dump = new File(path);

      boolean r = newfile.renameTo(dump);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }


  public static void main(String[] args) throws DBAppException {
    // String[] s = {"id","name"};
    //        Vector<Integer> v=new Vector<Integer>();
    //        v.add(0);
    //        v.add(1);
    //        String name="trialgpaname";
    //        createbucket(v,name);
    Object[] o = {3.9, "AAAA"};
  }
}
