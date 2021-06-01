import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

public class Bucket extends Vector<ArrayList<Object>> implements Serializable {

  Vector<Bucket> overflow;


  public Bucket() {
    overflow = new Vector<>();
  }

  public boolean isFull() {
    boolean res = false;
    Properties prop = new Properties();
    String fileName = "src/main/resources/DBApp.config";
    InputStream is = null;
    try {
      is = new FileInputStream(fileName);
    } catch (FileNotFoundException ignored) {

    }
    try {
      prop.load(is);
      int max = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
      if (this.size() == max) {
        res = true;
      } else {
        res = false;
      }
    } catch (IOException ex) {

    }

    return res;
  }

  public void serialB(String s) {
    try {
      String filename = "src/main/resources/data/" + s + ".ser";

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

  public static Bucket deserialB(String s) {
    Bucket p;
    try {
      FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + s + ".ser");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      p = (Bucket) in.readObject();
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
  public int binarysearch(Object key){
    int ret=-1;
    if (key instanceof Integer){
      ret=binarysearchint(this,(int)key);

      }
    else if(key instanceof String){
      ret=binarysearchstring(this,(String)key);
    }
    else if(key instanceof Date){
      ret=binarysearchdate(this,(Date)key);
    }
    else if(key instanceof Double){
     ret=binarysearchdouble(this,(double)key);
    }
    return ret;
  }

  public static int binarysearchint(Bucket b,int key) {
    int bucketsize = b.size();
    int low = 0;
    int high = bucketsize - 1;
    int mid = high / 2;
    while (low <= high) {

      int midVal = (int) (b.get(mid).get(3));

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
      mid = (low + high) / 2;
    }
    return -1;  // key not found  }

  }
  public static int binarysearchstring( Bucket b,String key) {
    int bucketsize = b.size();
    int low = 0;
    int high = b.size() - 1;
    int mid = (low + high) / 2;
    while (low <= high) {

      String midVal = (String) (b.get(mid).get(3));
      int comp = midVal.compareTo(key);

      if (comp < 0) {
        low = mid + 1;
      } else if (comp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
      mid = (low + high) / 2;
    }
    return -1;  // key not found
  }

  public static int binarysearchdate(Bucket b, Date key) {
    int bucketsize = b.size();
    int low = 0;
    int high = b.size() - 1;
    int mid = (low + high) / 2;
    while (low <= high) {

      Date midVal = (Date) (b.get(mid).get(3));
      int comp = midVal.compareTo(key);

      if (comp < 0) {
        low = mid + 1;
      } else if (comp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
      mid = (low + high) / 2;
    }
    return -1;  // key not found
  }

  public static int binarysearchdouble(Bucket b,Double key) {
    int low = 0;
    int high = b.size() - 1;
    int mid = (low + high) / 2;
    while (low <= high) {

      Double midVal = (Double) (b.get(mid).get(3));
      BigDecimal comp = BigDecimal.valueOf(midVal);
      BigDecimal key1 = BigDecimal.valueOf(key);
      int cond = comp.compareTo(key1);

      if (cond < 0) {
        low = mid + 1;
      } else if (cond == 0) {
        return mid; // key found
      } else {
        high = mid - 1;
      }

      mid = (low + high) / 2;
    }
    return -1;  // key not found
  }
}

