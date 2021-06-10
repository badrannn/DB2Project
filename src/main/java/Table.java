import java.io.*;
import java.util.Vector;

public class Table extends Vector<Page> implements Serializable {

  int len;
  String name;
  String cluster;
  String[] columns;

  public Table(String name) {
    this.name = name;
    len = 0;
  }

  public void serialT() {
    try {
      String dataDirPath = "src/main/resources/data/";
      String filename = dataDirPath + name + ".ser";

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

  public static void insertInto(String s) {
    Table t;
    try {
      FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + s + ".ser");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      t = (Table) in.readObject();
      t.len = t.len + 1;
      t.serialT();
      in.close();
      fileIn.close();

    } catch (IOException i) {
      i.printStackTrace();

    } catch (ClassNotFoundException c) {
      System.out.println(" class not found");
      c.printStackTrace();
    }
  }

  public static void deleteFrom(String s) {
    Table t;
    try {
      FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + s + ".ser");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      t = (Table) in.readObject();
      t.len = t.len - 1;
      t.serialT();
      in.close();
      fileIn.close();
    } catch (IOException i) {
      i.printStackTrace();
    } catch (ClassNotFoundException c) {
      System.out.println(" class not found");
      c.printStackTrace();
    }
  }

  public static int deserialT(String s) { // return size
    Table t;
    try {
      FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + s + ".ser");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      t = (Table) in.readObject();
      int si = t.len;
      in.close();
      fileIn.close();
      return si;
    } catch (IOException i) {
      i.printStackTrace();
      return -1;
    } catch (ClassNotFoundException c) {
      System.out.println(" class not found");
      c.printStackTrace();
      return -1;
    }
  }

  public static String returnCluster(String s) {
    Table t;
    try {
      FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + s + ".ser");
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

  public static String[] returnColumns(String s) {
    Table t;
    try {
      FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + s + ".ser");
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

  public static void main(String[] args) {}
}
