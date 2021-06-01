import java.awt.*;
import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

public class DBApp implements DBAppInterface {

  public void init() {

    try {
      removeBlanksCsv();
      BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

      if (br.readLine() == null) {
        System.out.println("No errors, and file empty");
        FileWriter pw = new FileWriter("src/main/resources/metadata.csv", true);
        pw.write("Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max \n");
        pw.close();
      }
      br.close();

      Path path = Paths.get("src/main/resources/data"); // create data directory
      Files.createDirectories(path);
      File range = new File("src/main/resources/data/range.txt");
      System.out.println("Range file:" + range.createNewFile());

    } catch (IOException e) {

      System.err.println("Failed to create directory!" + e.getMessage());
    }
  }

  public void createTable(
      String strTableName,
      String strClusteringKeyColumn,
      Hashtable<String, String> htblColNameType,
      Hashtable<String, String> htblColNameMin,
      Hashtable<String, String> htblColNameMax)
      throws DBAppException {
    try {
      if (tableExists(strTableName)) {
        throw new DBAppException();
      }
      this.appendCsv(
          strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
      Table t = new Table(strTableName);

      t.cluster = strClusteringKeyColumn;

      t.columns = new String[htblColNameType.size()];
      t.columns[0] = strClusteringKeyColumn;

      htblColNameType.remove(strClusteringKeyColumn);
      Enumeration<String> keys = htblColNameType.keys();
      for (int i = 1; i < t.columns.length; i++) {
        t.columns[i] = keys.nextElement();
      }

      t.serialT();

    } catch (IOException e) {

      e.printStackTrace();
    }
    String[] coll = new String[1];
    coll[0] = strClusteringKeyColumn;
    DBApp db = new DBApp();
    db.createIndex(strTableName, coll);
  }

  public void appendCsv(
      String name,
      String cluster,
      Hashtable<String, String> htblColNameType,
      Hashtable<String, String> htblColNameMin,
      Hashtable<String, String> htblColNameMax)
      throws IOException, DBAppException {

    removeBlanksCsv();
    FileWriter pw = new FileWriter("src/main/resources/metadata.csv", true);
    Enumeration<String> type = htblColNameType.keys();

    while (type.hasMoreElements()) {
      Boolean clus = false;
      StringBuilder builder = new StringBuilder();
      String col = type.nextElement();

      String typ = htblColNameType.get(col);
      String min = htblColNameMin.get(col);
      String max = htblColNameMax.get(col);
      if (typ == null || min == null || max == null) {
        throw new DBAppException();
      }

      if (col.equals(cluster)) {
        clus = true;
      }

      builder.append(
          name
              + ","
              + col
              + ","
              + typ
              + ","
              + clus.toString()
              + ","
              + "false,"
              + min
              + ","
              + max
              + "\n");
      System.out.println(clus.toString());
      pw.write(builder.toString());
    }

    pw.close();
    System.out.print("done!");
  }

  public static boolean tableExists(String name) {
    boolean res = false;

    String line = "";
    String splitBy = ",";
    try {
      BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        String line0 = row[0];

        if (line0.equalsIgnoreCase(name)) {
          res = true;
          break;
        }
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return res;
  }

  public static boolean checkMinMax(String table, String col, Object key) throws DBAppException {
    int type = getType(table, col); // int 0 Double 1 String 2 Date 3
    String min = "";
    String max = "";

    String s = "";
    String line = "";
    String splitBy = ",";
    try {
      BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        String line0 = row[0];
        String line1 = row[1];

        if (line0.equalsIgnoreCase(table) && line1.equalsIgnoreCase(col)) {

          min = row[5];
          max = row[6];
          break;
        }
      }
      br.close();
      if (type == 0) {
        int obj = (int) key;
        try {
          int mi = Integer.parseInt(min);
          int ma = Integer.parseInt(max);
          if (obj <= ma && obj >= mi) {
            return true;
          } else {
            return false;
          }
        } catch (Exception e) {
          throw new DBAppException();
        }
      } else if (type == 1) {
        Double obj = (Double) key;
        BigDecimal obj1 = BigDecimal.valueOf(obj);
        try {
          Double mi = Double.parseDouble(min);
          Double ma = Double.parseDouble(max);
          BigDecimal mi1 = BigDecimal.valueOf(mi);
          BigDecimal ma1 = BigDecimal.valueOf(ma);
          int comp1 = obj1.compareTo(mi1);
          int comp2 = obj1.compareTo(ma1);

          if (comp1 >= 0 && comp2 <= 0) {
            return true;
          } else {
            return false;
          }
        } catch (Exception e) {
          throw new DBAppException();
        }
      } else if (type == 3) {

        try {
          DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

          Date mi = format.parse(min);
          Date ma = format.parse(max);
          Date obj = (Date) key;

          int comp1 = obj.compareTo(mi);
          int comp2 = obj.compareTo(ma);

          if (comp1 >= 0 && comp2 <= 0) {
            return true;
          } else {
            return false;
          }
        } catch (Exception e) {
          throw new DBAppException();
        }
      } else {
        String obj = (String) key;
        int comp1 = obj.compareToIgnoreCase(min);
        int comp2 = obj.compareToIgnoreCase(max);

        if (comp1 >= 0 && comp2 <= 0) {
          return true;
        } else {
          return false;
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  public static boolean checkdeleted(String tableName, int pagenum) {
    if (returnRange(tableName + pagenum)[0] == null) {
      return true;
    }
    return false;
  }

  public static int searchinsert(String tableName, Object key)
      throws
          DBAppException { // returns page number if value is -1 then insert into first page if -2
    // then last page
    int tablesize = Table.deserialT(tableName);
    for (int i = 0; i < tablesize; i++) {
      while (checkdeleted(tableName, i)) {
        i++;
        tablesize++;
      }
      String[] range = returnRange(tableName + i);
      String min = range[0];
      String max = range[1];
      String cluster = Table.returnCluster(tableName);
      int type = DBApp.getType(tableName, cluster); // int 0 Double 1 String 2 Date 3
      if (type == 0) {
        Integer minn = Integer.valueOf(min);
        Integer maxx = Integer.valueOf(max);
        Integer keyy = (Integer) key;
        if (keyy < minn) {
          while (checkdeleted(tableName, i - 1)) {
            if (i == 0) {
              return -1;
            }
            i--;
          }
          return i - 1;
        } else if (minn < keyy && keyy < maxx) {
          return i;
        }
      } else if (type == 2) {
        String keyy = (String) key;
        int compmin = keyy.compareTo(min);
        int compmax = keyy.compareTo(max);
        if (compmin < 0) {
          while (checkdeleted(tableName, i - 1)) {
            if (i == 0) {
              return -1;
            }
            i--;
          }
          return i - 1;
        } else if (compmin > 0 && compmax < 0) {
          return i;
        }
      } else if (type == 3) {

        try {

          DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

          Date minn = dateFormat.parse(min);

          Date maxx = dateFormat.parse(max);

          Date keyy = (Date) key;
          int compmin = keyy.compareTo(minn);
          int compmax = keyy.compareTo(maxx);
          if (compmin < 0) {
            while (checkdeleted(tableName, i - 1)) {
              if (i == 0) {
                return -1;
              }
              i--;
            }
            return i - 1;
          } else if (compmin > 0 && compmax < 0) {
            return i;
          }
        } catch (ParseException e) {
          // TODO Auto-generated catch block
          throw new DBAppException();
        }
      } else if (type == 1) {
        Double minn = Double.valueOf(min);
        Double maxx = Double.valueOf(max);
        Double keyy = (Double) key;
        BigDecimal minbig = BigDecimal.valueOf(minn);
        BigDecimal maxbig = BigDecimal.valueOf(maxx);
        BigDecimal keybig = BigDecimal.valueOf(keyy);
        int compmin = keybig.compareTo(minbig);
        int compmax = keybig.compareTo(maxbig);
        if (compmin < 0) {
          while (checkdeleted(tableName, i - 1)) {
            if (i == 0) {
              return -1;
            }
            i--;
          }
          return i - 1;
        } else if (compmin > 0 && compmax < 0) {
          return i;
        }
      }
    }
    return -2;
  }

  public static int getType(String name, String col) { // int 0 Double 1 String 2 Date 3
    String s = "";
    String line = "";
    String splitBy = ",";
    try {
      BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        String line0 = row[0];
        String line1 = row[1];

        if (line0.equalsIgnoreCase(name) && line1.equalsIgnoreCase(col)) {

          s = row[2];
          break;
        }
      }
      br.close();
      if (s.equalsIgnoreCase("java.lang.Integer")) {
        return 0;
      } else if (s.equalsIgnoreCase("java.lang.Double")) {
        return 1;
      } else if (s.equalsIgnoreCase("java.lang.String")) {
        return 2;
      } else {
        return 3;
      }

    } catch (IOException e) {
      e.printStackTrace();
      return -1;
    }
  }

  @Override
  public void createIndex(String tableName, String[] columnNames) throws DBAppException {
    removeBlanksCsv();
    Hashtable h = new Hashtable();
    for (int i = 0; i < columnNames.length; i++) {
      h.put(columnNames[i], "");
    }
    if (!tableExists(tableName) || !checkColumns(tableName, h)) {
      throw new DBAppException();
    }
    new Grid(tableName, columnNames);
    for (int i = 0; i < columnNames.length; i++) {
      removeBlanksCsv();
      updateIndex(tableName, columnNames[i]);
    }
  }

  public static String[] returnRange(String pageName) {
    String[] ran = new String[2];
    String line = "";
    String splitBy = ",";
    String path = "src/main/resources/data/range.txt";
    try {
      BufferedReader br = new BufferedReader(new FileReader(path));
      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        String line0 = row[0];

        if (line0.equalsIgnoreCase(pageName)) {
          ran[0] = row[1];
          ran[1] = row[2];
          break;
        }
      }
      br.close();
      return ran;

    } catch (IOException e) {
      e.printStackTrace();
      return ran;
    }
  }

  public static void removeBlanksCsv() {
    Scanner file;
    PrintWriter writer;
    String path = "src/main/resources/metadata.csv";
    try {
      file = new Scanner(new File("src/main/resources/metadata.csv")); // sourcefile
      writer = new PrintWriter("tempp.txt"); // destinationfile
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
      File newfile = new File("tempp.txt");
      File dump = new File(path);

      boolean r = newfile.renameTo(dump);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static void removeBlanks() {
    Scanner file;
    PrintWriter writer;
    String path = "src/main/resources/data/range.txt";
    try {
      file = new Scanner(new File("src/main/resources/data/range.txt")); // sourcefile
      writer = new PrintWriter("tempp.txt"); // destinationfile
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
      File newfile = new File("tempp.txt");
      File dump = new File(path);

      boolean r = newfile.renameTo(dump);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static void pageRecord(String pageName) { // update min max
    removeBlanks();
    Page p = Page.deserialP(pageName);
    int lastIndex = p.size() - 1;
    Object mini = p.get(0).get(0);
    Object maxi = p.get(lastIndex).get(0);
    String mi;
    String ma;
    if (mini instanceof Date) {
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      Date miniii = (Date) mini;
      Date maxiii = (Date) maxi;
      mi = dateFormat.format(miniii);
      ma = dateFormat.format(maxiii);
    } else {
      mi = mini.toString();
      ma = maxi.toString();
    }
    String path = "src/main/resources/data/range.txt";
    String temp = "temp.txt";
    File oldfile = new File(path);
    File newfile = new File(temp);
    try {
      String name = "";
      String min = "";
      String max = "";

      String line = "";
      String splitBy = ",";

      FileWriter fw = new FileWriter(temp, true); // start
      BufferedWriter bw = new BufferedWriter(fw);
      PrintWriter pw = new PrintWriter(bw);

      BufferedReader br = new BufferedReader(new FileReader(path));

      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        name = row[0];
        min = row[1];
        max = row[2];
        if (name.equalsIgnoreCase(pageName)) {
          pw.println(name + "," + mi + "," + ma);
        } else {
          pw.println(name + "," + min + "," + max);
        }
      }

      br.close();

      pw.close();

      bw.close();

      fw.close();

      boolean d = oldfile.delete();

      System.out.println("delete status: " + d);

      File dump = new File(path);

      boolean r = newfile.renameTo(dump);

      System.out.println("Rename status: " + r);
    } catch (Exception e) {
      System.out.println("Error!");
    }
  }

  public static void deleteRecord(String record) {
    removeBlanks();
    String path = "src/main/resources/data/range.txt";
    String temp = "temp.txt";
    File oldfile = new File(path);
    File newfile = new File(temp);
    try {
      String name = "";
      String min = "";
      String max = "";

      String line = "";
      String splitBy = ",";

      FileWriter fw = new FileWriter(temp, true);
      BufferedWriter bw = new BufferedWriter(fw);
      PrintWriter pw = new PrintWriter(bw);

      BufferedReader br = new BufferedReader(new FileReader(path));

      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        name = row[0];
        min = row[1];
        max = row[2];
        if (!name.equalsIgnoreCase(record)) {
          pw.println(name + "," + min + "," + max);
        }
      }

      br.close();
      pw.close();
      bw.close();
      fw.close();

      boolean d = oldfile.delete();

      System.out.println("delete status: " + d);

      File dump = new File(path);

      boolean r = newfile.renameTo(dump);

      System.out.println("Rename status: " + r);
    } catch (Exception e) {
      System.out.println("Error!");
    }
  }

  public static boolean checkColumns(
      String tableName, Hashtable<String, Object> colNameValue) { // true if data consistent
    String[] columns = Table.returnColumns(tableName);
    Enumeration<String> keys = colNameValue.keys();

    while (keys.hasMoreElements()) {
      boolean elem = false;
      String key = keys.nextElement();
      for (int i = 0; i < columns.length; i++) {
        if (key.equalsIgnoreCase(columns[i])) {
          elem = true;
          break;
        }
      }
      if (elem == false) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue)
      throws DBAppException {

    boolean checkTable = tableExists(tableName);
    if (!checkTable) {
      throw new DBAppException();
    }
    Comparator<ArrayList<Object>> comparator = new Com();

    ArrayList<Object> row = new ArrayList<>(); // record we wanna insert

    String cluster = Table.returnCluster(tableName); // cluster column
    String[] columns = Table.returnColumns(tableName);

    boolean checkClms = checkColumns(tableName, colNameValue);
    if (checkClms == false) { // false column name was specified (key)
      throw new DBAppException();
    }

    if (!(colNameValue.containsKey(cluster))) { // no cluster key was inserted
      throw new DBAppException();
    }

    Object valu = colNameValue.get(cluster); // cluster key value
    if (valu == null) { // check if cluster value = null
      throw new DBAppException();
    }

    boolean exist = insertexist(tableName, valu);
    // int pageNum= index[0];

    if ((exist)) { // check if cluster value = null or cluster value exists already in table
      throw new DBAppException();
    }

    for (int i = 0; i < columns.length; i++) {
      String col = columns[i];
      Object x = colNameValue.get(col);
      int ty = getType(tableName, col); // int 0 Double 1 String 2 Date 3
      if (x == null) {
        row.add(x);
      } else if (((x instanceof Integer) && ty != 0)
          || ((x instanceof Double) && ty != 1)
          || ((x instanceof String) && ty != 2)
          || ((x instanceof Date) && ty != 3)) {
        throw new DBAppException();
      } else {
        boolean check = checkMinMax(tableName, col, x);
        if (!check) {
          throw new DBAppException();
        }
        row.add(x);
      }
    } // done with the tuple related exceptions and ready to insert

    // check if table is empty or not
    if (Table.deserialT(tableName) == 0) {
      try {
        FileWriter pw = new FileWriter("src/main/resources/data/range.txt", true);
        Page p = new Page();
        StringBuilder builder = new StringBuilder();
        builder.append(tableName + 0 + "," + " " + "," + " " + "\n");
        pw.write(builder.toString());
        pw.close();

        p.add(row);
        p.serialP(tableName + 0);
        Table.insertInto(tableName);
        pageRecord(tableName + 0);

      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      int pnum = searchinsert(tableName, valu); // where we should insert the tuple

      if (pnum == -1) { // insert in first page
        int i = 0;
        while (checkdeleted(tableName, i)) {
          i++;
        }

        Page p = Page.deserialP(tableName + i);
        if (!p.isFull()) {
          p.add(row);
          Collections.sort(p, comparator);
          p.serialP(tableName + i);
          pageRecord(tableName + i);
        } else { // first check if there's a next page
          if (Table.deserialT(tableName) == 1) {
            try {
              int j = i + 1;
              FileWriter pw = new FileWriter("src/main/resources/data/range.txt", true);
              StringBuilder builder = new StringBuilder();
              builder.append("\n" + tableName + j + "," + "null " + "," + " null");
              pw.write(builder.toString());

              pw.close();

              ArrayList removed = p.remove(p.size() - 1);
              Page next = new Page();
              next.add(removed);
              p.add(row);

              Collections.sort(p, comparator);

              next.serialP(tableName + j);
              p.serialP(tableName + i);

              pageRecord(tableName + j);
              pageRecord(tableName + i);

              Table.insertInto(tableName);

            } catch (IOException e) {
              e.printStackTrace();
            }

          } else {
            if (p.overflow.size() > 0) {
              Page oo = null;
              for (int k = 0; k < p.overflow.size(); k++) {
                oo = p.overflow.get(k);
                if (!oo.isFull()) {
                  break;
                }
                oo = null;
              }
              if (oo == null) {
                oo = new Page();
                ArrayList removed = p.remove(0);
                p.add(row);
                oo.add(removed);
                Collections.sort(p, comparator);

                p.addOverflow(oo);
                p.serialP(tableName + i);
                pageRecord(tableName + i);

              } else {
                ArrayList removed = p.remove(0);
                p.add(row);
                oo.add(removed);
                Collections.sort(p, comparator);
                Collections.sort(oo, comparator);

                p.serialP(tableName + i);
                pageRecord(tableName + i);
              }

            } else {
              int j = i + 1;
              while (checkdeleted(tableName, j)) {
                j++;
              }

              Page ne = Page.deserialP(tableName + j);

              if (ne.isFull()) { // overflow

                Page oo = new Page();
                ArrayList removed = p.remove(0);
                p.add(row);
                oo.add(removed);
                Collections.sort(p, comparator);

                p.addOverflow(oo);
                p.serialP(tableName + i);
                pageRecord(tableName + i);
              } else {
                ArrayList removed = p.remove(p.size() - 1);
                ne.add(removed);
                p.add(row);
                Collections.sort(p, comparator);
                Collections.sort(ne, comparator);
                p.serialP(tableName + i);
                ne.serialP(tableName + j);
                pageRecord(tableName + i);
                pageRecord(tableName + j);
              }
            }
          }
        }

      } else if (pnum == -2) {
        int i = 0;
        int dum = Table.deserialT(tableName);
        while (i < dum) {
          if (checkdeleted(tableName, i)) {
            i++;
            dum++;
          } else {
            i++;
          }
        }
        i = i - 1;
        Page p = Page.deserialP(tableName + i);
        if (p.isFull()) {
          try {
            int newPi = i + 1;

            FileWriter pw = new FileWriter("src/main/resources/data/range.txt", true);
            StringBuilder builder = new StringBuilder();
            builder.append("\n" + tableName + "" + newPi + "" + "," + "null" + "," + "null");
            pw.write(builder.toString());

            pw.close();

            Page newP = new Page();

            newP.add(row);

            Table.insertInto(tableName);

            newP.serialP(tableName + newPi);

            pageRecord(tableName + newPi);

          } catch (IOException e) {
            e.printStackTrace();
          }

        } else {
          p.add(row);

          Collections.sort(p, comparator);

          p.serialP(tableName + i);

          pageRecord(tableName + i);
        }

      } else {

        Page p = Page.deserialP(tableName + pnum);

        if (!p.isFull()) {
          p.add(row);
          Collections.sort(p, comparator);
          p.serialP(tableName + pnum);
          pageRecord(tableName + pnum);
        } else {
          int i = 0;
          int dum = Table.deserialT(tableName);
          while (i < dum) {
            if (checkdeleted(tableName, i)) {
              i++;
              dum++;
            } else {
              i++;
            }
          }
          i = i - 1;
          if (pnum == i) {
            int newPi = i + 1;
            try {
              FileWriter pw = new FileWriter("src/main/resources/data/range.txt", true);
              StringBuilder builder = new StringBuilder();
              builder.append("\n" + tableName + "" + newPi + "" + "," + "null" + "," + "null");
              pw.write(builder.toString());

              pw.close();

              Page newP = new Page();
              Table.insertInto(tableName);
              ArrayList removed = p.remove(p.size() - 1);

              newP.add(removed);
              p.add(row);
              Collections.sort(p, comparator);
              p.serialP(tableName + pnum);
              newP.serialP(tableName + newPi);
              pageRecord(tableName + pnum);
              pageRecord(tableName + newPi);

            } catch (Exception e) {
              e.printStackTrace();
            }
          } else {
            Vector<ArrayList<Object>> temm = new Vector<>();
            ArrayList r = p.remove(p.size() - 1);
            temm.add(row);
            temm.add(r);
            Collections.sort(temm, comparator);

            if (p.overflow.size() > 0) {
              Page oo = null;
              for (int k = 0; k < p.overflow.size(); k++) {
                oo = p.overflow.get(k);
                if (!oo.isFull()) {
                  break;
                }
                oo = null;
              }
              if (oo == null) {
                oo = new Page();
                p.add(temm.get(1));
                oo.add(temm.get(0));
                Collections.sort(p, comparator);

                p.addOverflow(oo);
                p.serialP(tableName + pnum);
                pageRecord(tableName + pnum);

              } else {
                p.add(temm.get(1));
                oo.add(temm.get(0));
                Collections.sort(p, comparator);
                Collections.sort(oo, comparator);

                p.serialP(tableName + pnum);
                pageRecord(tableName + pnum);
              }

            } else {

              int j = pnum + 1;
              while (checkdeleted(tableName, j)) {
                j++;
              }

              Page next = Page.deserialP(tableName + j);

              if (next.isFull()) {

                Page oo = new Page();
                p.add(temm.get(1));
                oo.add(temm.get(0));
                Collections.sort(p, comparator);

                p.addOverflow(oo);
                p.serialP(tableName + pnum);
                pageRecord(tableName + pnum);

              } else {

                next.add(temm.get(1));
                p.add(temm.get(0));
                Collections.sort(p, comparator);
                Collections.sort(next, comparator);
                p.serialP(tableName + pnum);
                next.serialP(tableName + j);
                pageRecord(tableName + pnum);
                pageRecord(tableName + j);
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void updateTable(
      String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
      throws DBAppException {
    boolean check = checkColumns(tableName, columnNameValue);
    if (!check) {
      throw new DBAppException();
    }
    boolean flagover = false;
    Page overflow = null;
    int k = 0;
    String[] columns = Table.returnColumns(tableName);
    int type = getType(tableName, columns[0]);
    Object clusterkey = -1;
    if (type == 0) {
      clusterkey = Integer.parseInt(clusteringKeyValue);
    } else if (type == 1) {
      clusterkey = Double.parseDouble(clusteringKeyValue);
    } else if (type == 3) {
      DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

      try {
        clusterkey = format.parse(clusteringKeyValue);
      } catch (ParseException e) {
        throw new DBAppException();
      }

    } else {
      clusterkey = clusteringKeyValue;
    }
    int[] pos = searchtable(tableName, clusterkey);
    if (pos[0] >= 0 && pos[1] == -1) {
      flagover = true;
      Page p = Page.deserialP(tableName + pos[0]);
      for (k = 0; k < p.overflow.size(); k++) {
        overflow = p.overflow.get(k);
        if (clusterkey instanceof Integer) {
          pos[1] = binarysearchint(overflow, (int) clusterkey);
        } else if (clusterkey instanceof String) {
          pos[1] = binarysearchstring(overflow, (String) clusterkey);
        } else if (clusterkey instanceof Double) {
          pos[1] = binarysearchdouble(overflow, (double) clusterkey);
        } else if (clusterkey instanceof Date) {
          pos[1] = binarysearchdate(overflow, (Date) clusterkey);
        }
        if (pos[1] >= 0) {
          break;
        }
      }
    }
    if (pos[0] == -1 || pos[1] == -1) {
      return;
    }
    Enumeration<String> keys = columnNameValue.keys();
    String s = tableName + pos[0];
    Page p = Page.deserialP(s);
    while (keys.hasMoreElements()) {
      String key = keys.nextElement();
      int j = coloumnnum(key, tableName);
      if (j == -1) {
        throw new DBAppException();
      }
      Object Change = columnNameValue.get(key);
      if (!checkMinMax(tableName, columns[j], Change)) {
        throw new DBAppException();
      }

      if (!flagover) {
        p.get(pos[1]).set(j, Change);
      } else {
        p.overflow.get(k).get(pos[1]).set(j, Change);
      }
    }

    p.serialP(s);
  }

  public void updateTablewithindex(
      String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
      throws DBAppException {
    boolean check = checkColumns(tableName, columnNameValue);
    if (!check) {
      throw new DBAppException();
    }
    boolean flagover = false;
    Page overflow = null;
    int k = 0;
    String[] columns = Table.returnColumns(tableName);
    int type = getType(tableName, columns[0]);
    Object clusterkey = -1;
    if (type == 0) {
      clusterkey = Integer.parseInt(clusteringKeyValue);
    } else if (type == 1) {
      clusterkey = Double.parseDouble(clusteringKeyValue);
    } else if (type == 3) {
      DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

      try {
        clusterkey = format.parse(clusteringKeyValue);
      } catch (ParseException e) {
        throw new DBAppException();
      }

    } else {
      clusterkey = clusteringKeyValue;
    }
    String s = tableName + columns[0];
    Grid g = Grid.deserialG(s);
    ArrayList<Object> inserted = new ArrayList<Object>();
    inserted.add(clusterkey);
    Vector<Integer> cell = g.returnCell(s, inserted);
    Object[] gg = g.grid;
    Bucket b = Grid.returnbuck(gg, cell, 0);
    ArrayList<Object> t = new ArrayList<Object>();
    int ret = b.binarysearch(clusterkey);
    if (ret == -1) {
      for (int i = 0; i < b.overflow.size(); i++) {
        Bucket overbuck = b.overflow.get(i);
        ret = overbuck.binarysearch(clusterkey);
        if (ret != -1) {
          t = overbuck.get(ret);
          break;
        }
      }
    } else {
      t = b.get(ret);
    }

    int[] pos = new int[2];
    pos[0] = (int) t.get(0);
    pos[1] = (int) t.get(1);
    flagover = (boolean) t.get(2);
    k = (int) t.get(3);
    Enumeration<String> keys = columnNameValue.keys();
    String ss = tableName + pos[0];
    Page p = Page.deserialP(ss);
    while (keys.hasMoreElements()) {
      String key = keys.nextElement();
      int j = coloumnnum(key, tableName);
      if (j == -1) {
        throw new DBAppException();
      }
      Object Change = columnNameValue.get(key);
      if (!checkMinMax(tableName, columns[j], Change)) {
        throw new DBAppException();
      }

      if (!flagover) {
        p.get(pos[1]).set(j, Change);
      } else {
        p.overflow.get(k).get(pos[1]).set(j, Change);
      }
    }

    p.serialP(ss);
  }

  /*public static void rename(String tableName,int pagenum){
  	int tablesize=Table.deserialT(tableName);
  	if(pagenum==tablesize-1){
  		File f1 = new File("src/main/resources/data"+tableName+pagenum+".ser");
  		 boolean b=f1.delete();
  		 System.out.println(b);
  	}
  	else{
  	for(int i=pagenum;i<(tablesize-1);i++){
  		String path1="src/main/resources/data/"+tableName+i+".ser";
  		int j=i+1;
  		String path2="src/main/resources/data/"+tableName+j+".ser";
  		File f1 = new File(path1);
  		File f2 = new File(path2);
  		boolean b=f2.renameTo(f1);
  		System.out.println(b);
  	}
  		int pp = tablesize-1;
  		File f1 = new File("src/main/resources/data"+tableName+pp+".ser");
  		boolean bb=f1.delete();
  		System.out.println(bb);

  }

  	//modify tablesize after

  }
  */
  //	public static void insertintoindex(String Gridname,Vector<Integer>
  // bucketNumber,ArrayList<Object> adding){
  //		String bucketref=Grid.checkBucket(bucketNumber,Gridname);
  //		Bucket buck=Bucket.deserialB(bucketref);
  //		if(buck.isFull()){
  //			boolean overflow=false;
  //			for (int i = 0; i <buck.overflow.size() ; i++) {
  //				if(!buck.overflow.get(i).isFull()){
  //					overflow=true;
  //					buck.overflow.get(i).add(adding);
  //					break;
  //				}
  //			}
  //			if(!overflow){
  //				Bucket over=new Bucket();
  //				over.add(adding);
  //				buck.overflow.add(over);
  //			}
  //		}
  //		else{
  //			buck.add(adding);
  //		}
  //		buck.serialB(bucketref);
  //	}
  public static void overminmax(Page p) {
    Comparator<ArrayList<Object>> comparator = new Com();
    Object maximum = p.get(p.size() - 1).get(0);
    Object minimum = p.get(0).get(0);
    Object maaa = p.get(p.size() - 1).get(0);
    Object miii = p.get(0).get(0);
    int maxpos = -1;
    int minpos = -1;
    Vector<Page> overflow = p.overflow;
    Page over;
    for (int i = 0; i < overflow.size(); i++) {
      over = p.overflow.get(i);
      if (maximum instanceof Integer) {
        int max = (int) maximum;
        int min = (int) minimum;
        Object overma = over.get(over.size() - 1).get(0);
        int overmax = (int) overma;
        Object overmi = over.get(0).get(0);
        int overmin = (int) overmi;
        if (overmax > max) {
          maxpos = i;
          maximum = overmax;
        }
        if (overmin < min) {
          minpos = i;
          minimum = overmin;
        }
      } else if (maximum instanceof String) {
        String max = (String) maximum;
        String min = (String) minimum;
        Object overma = over.get(over.size() - 1).get(0);
        String overmax = (String) overma;
        Object overmi = over.get(0).get(0);
        String overmin = (String) overmi;
        int compmin = overmin.compareTo(min);
        int compmax = overmax.compareTo(max);
        if (compmax > 0) {
          maxpos = i;
          maximum = overmax;
        }
        if (compmin < 0) {
          minpos = i;
          minimum = overmin;
        }

      } else if (maximum instanceof Double) {
        Double max = (Double) maximum;
        Double min = (Double) minimum;
        Object overma = over.get(over.size() - 1).get(0);
        Double overmax = (Double) overma;
        Object overmi = over.get(0).get(0);
        Double overmin = (Double) overmi;
        BigDecimal comparemin = new BigDecimal(overmin);
        BigDecimal comparemax = new BigDecimal(overmax);
        BigDecimal minnn = new BigDecimal(min);
        BigDecimal maxxx = new BigDecimal(max);
        int compmin = comparemin.compareTo(minnn);
        int compmax = comparemax.compareTo(maxxx);
        if (compmax > 0) {
          maxpos = i;
          maximum = overma;
        }
        if (compmin < 0) {
          minpos = i;
          minimum = overmi;
        }

      } else if (maximum instanceof Date) {
        Date max = (Date) maximum;
        Date min = (Date) minimum;
        Object overma = over.get(over.size() - 1).get(0);
        Date overmax = (Date) overma;
        Object overmi = over.get(0).get(0);
        Date overmin = (Date) overmi;
        int compmin = overmin.compareTo(min);
        int compmax = overmax.compareTo(max);
        if (compmax > 0) {
          maxpos = i;
          maximum = overmax;
        }
        if (compmin < 0) {
          minpos = i;
          minimum = overmin;
        }
      }
    }
    if (maaa == miii) {
      if (minpos >= 0) {
        ArrayList rem = p.overflow.get(minpos).remove(0);
        p.add(rem);
        Collections.sort(p, comparator);
        if (p.overflow.get(minpos).size() == 0) {
          p.overflow.removeElementAt(minpos);
        }
      }
      if (maxpos >= 0) {
        ArrayList rem = p.overflow.get(maxpos).remove(p.overflow.get(maxpos).size() - 1);
        p.add(rem);
        Collections.sort(p, comparator);
        if (p.overflow.get(maxpos).size() == 0) {
          p.overflow.removeElementAt(maxpos);
        }
      }
    } else {
      if (minpos >= 0) {
        ArrayList r = p.remove(0);
        ArrayList rem = p.overflow.get(minpos).remove(0);
        p.add(rem);
        p.overflow.get(minpos).add(r);
        Collections.sort(p, comparator);
        Collections.sort(p.overflow.get(minpos), comparator);
      }
      if (maxpos >= 0) {
        ArrayList r = p.remove(p.size() - 1);
        ArrayList rem = p.overflow.get(maxpos).remove(p.overflow.get(maxpos).size() - 1);
        p.add(rem);
        p.overflow.get(maxpos).add(r);
        Collections.sort(p, comparator);
        Collections.sort(p.overflow.get(maxpos), comparator);
      }
    }
  }

  @Override
  public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue)
      throws DBAppException {
    boolean check = checkColumns(tableName, columnNameValue);
    if (!check) {
      throw new DBAppException();
    }
    Enumeration<String> key = columnNameValue.keys();
    boolean flagg = false;
    Object cluster = null;
    Page overflow = null;
    boolean flagover = false;
    int e = 0;
    while (key.hasMoreElements()) {
      String col = key.nextElement();
      if (col.equals(Table.returnCluster(tableName))) {
        flagg = true;
        cluster = columnNameValue.get(col);
        break;
      }
    }
    if (flagg) {
      int[] A = searchtable(tableName, cluster);
      if (A[0] >= 0 && A[1] == -1) {
        flagover = true;
        Page p = Page.deserialP(tableName + A[0]);
        for (e = 0; e < p.overflow.size(); e++) {
          overflow = p.overflow.get(e);
          if (cluster instanceof Integer) {
            A[1] = binarysearchint(overflow, (int) cluster);
          } else if (cluster instanceof String) {
            A[1] = binarysearchstring(overflow, (String) cluster);
          } else if (cluster instanceof Double) {
            A[1] = binarysearchdouble(overflow, (double) cluster);
          } else if (cluster instanceof Date) {
            A[1] = binarysearchdate(overflow, (Date) cluster);
          }
          if (A[1] >= 0) {
            break;
          }
        }
      }

      if (A[0] == -1 || A[1] == -1) {
        return;
      }
      ArrayList<Object> ele = null;
      if (flagover) {
        ele = Page.deserialP(tableName + A[0]).overflow.get(e).get(A[1]);
      } else {
        ele = Page.deserialP(tableName + A[0]).get(A[1]);
      }
      key = columnNameValue.keys();
      boolean flag = true;
      while (key.hasMoreElements()) {
        String col = key.nextElement();
        int B = coloumnnum(col, tableName);
        Object value = columnNameValue.get(col);
        if (value instanceof Integer) {
          int int1 = (int) value;
          int int2 = (int) ele.get(B);
          if (!(int2 == int1)) {
            flag = false;
            break;
          }
        } else if (value instanceof String) {
          String str = (String) value;
          String Str2 = (String) ele.get(B);
          if (!(Str2.equals(str))) {
            flag = false;
            break;
          }
        } else if (value instanceof Date) {
          Date dt = (Date) value;
          Date dt2 = (Date) ele.get(B);
          if (!(dt.equals(dt2))) {
            flag = false;
            break;
          }
        } else if (value instanceof Double) {
          String srs = value.toString();
          Double doo = Double.valueOf(srs);
          BigDecimal big = BigDecimal.valueOf(doo);
          String srs1 = ele.get(B).toString();
          Double doo1 = Double.valueOf(srs1);
          BigDecimal big1 = BigDecimal.valueOf(doo1);
          if (!(big1.equals(big))) {
            flag = false;
            break;
          }
        }
      }
      if (flag == true) {
        if (!flagover) { // deleting from page
          Page p = Page.deserialP(tableName + A[0]);
          p.removeElementAt(A[1]);
          if (p.isEmpty()) {
            if (p.overflow.size() == 0) { // if there no overflow
              deleteRecord(tableName + A[0]);
              String path = "src/main/resources/data/" + tableName + A[0] + ".ser";
              File f = new File(path);
              f.delete();
              Table.deleteFrom(tableName);
            } else { // if removing a page and there exists an overflow
              Vector<Page> overrr = p.overflow;
              deleteRecord(tableName + A[0]);
              String path = "src/main/resources/data/" + tableName + A[0] + ".ser";
              Table.deleteFrom(tableName);
              File f = new File(path);
              f.delete();
              Page ove1 = overrr.get(0);
              for (int i = 1; i < overrr.size(); i++) {
                Page ove2 = overrr.get(i);
                ove1.overflow.add(ove2);
              }

              ove1.serialP(tableName + A[0]);
              overminmax(ove1);
              ove1.serialP(tableName + A[0]);
              pageRecord(tableName + A[0]);
            }

          } else { // if removing a record
            if (p.overflow.size() > 0) {
              p.serialP(tableName + A[0]);
              Page p1 = Page.deserialP(tableName + A[0]);
              overminmax(p1);

              p1.serialP(tableName + A[0]);
              pageRecord(tableName + A[0]);

            } else {
              p.serialP(tableName + A[0]);
              pageRecord(tableName + A[0]);
            }
          }
        } else { // deleting from overflow
          Page p1 = Page.deserialP(tableName + A[0]);
          p1.overflow.get(e).removeElementAt(A[1]);
          Page p = p1.overflow.get(e);
          if (p.isEmpty()) {
            p1.overflow.removeElementAt(e);
          }
          p1.serialP(tableName + A[0]);
        }
      }

    } else {
      int tablesize = Table.deserialT(tableName);
      for (int i = 0; i < tablesize; i++) {
        while (checkdeleted(tableName, i)) {
          i++;
          tablesize++;
        }
        Page f = Page.deserialP(tableName + i);
        int pagesize = f.size();
        int j = 0;
        int m = 0;
        int iverr = f.overflow.size();
        while (m < iverr) { // m pages overflow
          int x = 0;
          int oversize = f.overflow.get(m).size();
          while (x < oversize) { // tuples
            key = columnNameValue.keys();
            boolean flag = true;
            while (key.hasMoreElements()) {
              String col = key.nextElement();
              int A = coloumnnum(col, tableName);
              Object value = columnNameValue.get(col);
              if (value instanceof Integer) {
                int int1 = (int) value;
                int int2 = (int) f.overflow.get(m).get(x).get(A);
                if (!(int2 == int1)) {
                  flag = false;
                  break;
                }
              } else if (value instanceof String) {
                String str = (String) value;
                String Str2 = (String) f.overflow.get(m).get(x).get(A);
                if (!(Str2.equals(str))) {
                  flag = false;
                  break;
                }
              } else if (value instanceof Date) {
                Date dt = (Date) value;
                Date dt2 = (Date) f.overflow.get(m).get(x).get(A);
                if (!(dt.equals(dt2))) {
                  flag = false;
                  break;
                }
              } else if (value instanceof Double) {
                String srs = value.toString();
                Double doo = Double.valueOf(srs);
                BigDecimal big = BigDecimal.valueOf(doo);
                String srs1 = f.overflow.get(m).get(x).get(A).toString();
                Double doo1 = Double.valueOf(srs1);
                BigDecimal big1 = BigDecimal.valueOf(doo1);
                if (!(big1.equals(big))) {
                  flag = false;
                  break;
                }
              }
            }
            if (flag) {
              f.overflow.get(m).removeElementAt(x);
              oversize--;
              if (f.overflow.get(m).size() == 0) {
                break;
              }

            } else {
              x++;
            }
          }
          if (f.overflow.get(m).size() == 0) {
            iverr--;
            f.overflow.removeElementAt(m);
          } else {
            m++;
          }
        }
        while (j < pagesize) {
          key = columnNameValue.keys();
          boolean flag = true;
          while (key.hasMoreElements()) {
            String col = key.nextElement();
            int A = coloumnnum(col, tableName);
            Object value = columnNameValue.get(col);
            if (value instanceof Integer) {
              int int1 = (int) value;
              int int2 = (int) f.get(j).get(A);
              if (!(int2 == int1)) {
                flag = false;
                break;
              }
            } else if (value instanceof String) {
              String str = (String) value;
              String Str2 = (String) f.get(j).get(A);
              if (!(Str2.equals(str))) {
                flag = false;
                break;
              }
            } else if (value instanceof Date) {
              Date dt = (Date) value;
              Date dt2 = (Date) f.get(j).get(A);
              if (!(dt.equals(dt2))) {
                flag = false;
                break;
              }
            } else if (value instanceof Double) {
              String srs = value.toString();
              Double doo = Double.valueOf(srs);
              BigDecimal big = BigDecimal.valueOf(doo);
              String srs1 = f.get(j).get(A).toString();
              Double doo1 = Double.valueOf(srs1);
              BigDecimal big1 = BigDecimal.valueOf(doo1);
              if (!(big1.equals(big))) {
                flag = false;
                break;
              }
            }
          }

          if (flag == true) {

            f.removeElementAt(j);
            pagesize = f.size();
            if (f.isEmpty()) {
              if (f.overflow.size() > 0) {
                Vector<Page> overrr = f.overflow;
                String path = "src/main/resources/data/" + tableName + i + ".ser";
                File z = new File(path);
                z.delete();
                Page ove1 = overrr.get(0);
                for (int g = 1; g < overrr.size(); g++) {
                  Page ove2 = overrr.get(g);
                  ove1.overflow.add(ove2);
                }

                overminmax(ove1);
                ove1.serialP(tableName + i);
                pageRecord(tableName + i);
                break;

              } else {
                deleteRecord(tableName + i);
                String path = "src/main/resources/data/" + tableName + i + ".ser";
                File k = new File(path);
                k.delete();
                Table.deleteFrom(tableName);
                break;
              }
            }

          } else {
            j++;
          }
        }
        if (f.size() > 0) {
          overminmax(f);
          f.serialP(tableName + i);
          pageRecord(tableName + i);
        }
      }
    }
  }

  public static void updateIndex(String tname, String col) {
    removeBlanks();

    String path = "src/main/resources/metadata.csv";
    String temp = "tem.txt";
    File oldfile = new File(path);
    File newfile = new File(temp);
    try {
      String name = "";
      String colu = "";
      String indexd = "";

      String line = "";
      String splitBy = ",";

      FileWriter fw = new FileWriter(temp, true); // start
      BufferedWriter bw = new BufferedWriter(fw);
      PrintWriter pw = new PrintWriter(bw);

      BufferedReader br = new BufferedReader(new FileReader(path));

      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        name = row[0];
        colu = row[1];
        indexd = row[4];

        if (name.equalsIgnoreCase(tname) && colu.equals(col) && indexd.equals("false")) {
          pw.println(
              name + "," + colu + "," + row[2] + "," + row[3] + "," + "true" + "," + row[5] + ","
                  + row[6]);
        } else {
          pw.println(
              name + "," + colu + "," + row[2] + "," + row[3] + "," + row[4] + "," + row[5] + ","
                  + row[6]);
        }
      }

      br.close();

      pw.close();

      bw.close();

      fw.close();

      boolean d = oldfile.delete();

      System.out.println("delete status: " + d);

      File dump = new File(path);

      boolean r = newfile.renameTo(dump);

      System.out.println("Rename status: " + r);
    } catch (Exception e) {
      System.out.println("Error!");
    }
  }

  @Override
  public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators)
      throws DBAppException {

    for (int i = 0; i < sqlTerms.length; i++) {
      String tname = sqlTerms[i]._strTableName;
      Hashtable<String, Object> h = new Hashtable();
      h.put(sqlTerms[i]._strColumnName, "");

      int ty = getType(tname, sqlTerms[i]._strColumnName);
      Object x = sqlTerms[i]._objValue;

      if (!checkColumns(tname, h) || !tableExists(tname)) {
        throw new DBAppException();
      }
      if (((x instanceof Integer) && ty != 0)
          || ((x instanceof Double) && ty != 1)
          || ((x instanceof String) && ty != 2)
          || ((x instanceof Date) && ty != 3)) {
        throw new DBAppException();
      }
    }
    Vector<Vector<ArrayList<Object>>> intermid = new Vector<Vector<ArrayList<Object>>>();

    for (SQLTerm sq : sqlTerms) {
      intermid.add(execterm(sq));
    }
    Arrays.sort(arrayOperators); // sort AND OR XOR
    List<String> list = Arrays.asList(arrayOperators);
    while (intermid.size() > 1) {
      intermid.add(0, execOperator(intermid.remove(0), intermid.remove(1), list.remove(0)));
    }
    Iterator value = intermid.get(0).iterator();
    return value;
  }

  public static Vector<ArrayList<Object>> execOperator(
      Vector<ArrayList<Object>> a, Vector<ArrayList<Object>> b, String op) {
    switch (op) {
      case "AND":
        return execAnd(a, b);
      case "OR":
        return execOR(a, b);

      case "XOR":
        return execXOR(a, b);

      default:
        return null;
    }
  }

  public static Vector<ArrayList<Object>> execXOR(
      Vector<ArrayList<Object>> a, Vector<ArrayList<Object>> b) {

    Vector<ArrayList<Object>> returned = new Vector<ArrayList<Object>>();
    for (ArrayList<Object> i : a) {
      for (ArrayList<Object> j : b) {
        if (!i.equals(j)) returned.add(i);
      }
    }
    return returned;
  }

  public static Vector<ArrayList<Object>> execAnd(
      Vector<ArrayList<Object>> a, Vector<ArrayList<Object>> b) {
    Vector<ArrayList<Object>> returned = new Vector<ArrayList<Object>>();
    for (ArrayList<Object> i : a) {
      for (ArrayList<Object> j : b) {
        if (i.equals(j)) returned.add(i);
      }
    }
    return returned;
  }

  public static Vector<ArrayList<Object>> execOR(
      Vector<ArrayList<Object>> a, Vector<ArrayList<Object>> b) {
    Vector<ArrayList<Object>> returned = new Vector<ArrayList<Object>>();
    for (int i = 0; i < a.size(); i++) {
      for (int j = 0; j < b.size(); j++) {
        if (a.get(i).equals(b.get(j))) a.remove(i);
      }
    }
    returned.addAll(a);
    returned.addAll(b);
    return returned;
  }

  public Vector<ArrayList<Object>> execterm(SQLTerm sqlTerm) throws DBAppException {
    String oper = sqlTerm._strOperator;
    switch (oper) {
      case ">":
        return execgreateq(sqlTerm);

      case "=":
        return execEq(sqlTerm);

      case "<":
        return execless(sqlTerm);

      case ">=":
        return execgreateq(sqlTerm);

      case " <=":
        return execlesseq(sqlTerm);

      case "!=":
        execnoteq(sqlTerm);

      default:
        return null;
    }
  }

  public Vector<ArrayList<Object>> execgreat(SQLTerm sqlTerm) throws DBAppException {
    Vector<ArrayList<Object>> result = new Vector<ArrayList<Object>>();
    Object valu = sqlTerm._objValue;
    String tname = sqlTerm._strTableName;
    String col = sqlTerm._strColumnName;
    int i = getType(tname, col); // int 0 Double 1 String 2 Date 3
    String index1d = tname + col;
    String filePathString = "src/main/resources/data/" + index1d + ".ser";
    File f = new File(filePathString);
    if (f.exists() && !f.isDirectory()) {
      ArrayList<Object> a = new ArrayList<>();
      a.add(valu);
      Grid g = Grid.deserialG(index1d);
      assert g != null;
      Vector<Integer> v = g.returnCell(index1d, a);
      Bucket bj = Grid.returnbuck(g.grid, v, 0);
      if (bj != null) {
        for (ArrayList<Object> ao : bj) {
          Object vall = ao.get(5);
          boolean flag = false;
          if (i == 0) {
            if ((int) vall > (int) valu) flag = true;
          } else if (i == 1) {
            BigDecimal b = BigDecimal.valueOf((Double) vall);
            BigDecimal b1 = BigDecimal.valueOf((Double) valu);
            int k = b.compareTo(b1);
            if (k > 0) flag = true;
          } else if (i == 2) {
            int k = ((String) vall).compareTo((String) valu);
            if (k > 0) flag = true;
          } else {
            int k = ((Date) vall).compareTo((Date) valu);
            if (k > 0) flag = true;
          }
          if (flag) {
            int pnum = (int) ao.get(0);
            int tpnum = (int) ao.get(1);
            boolean of = (boolean) ao.get(2);
            int pnumof = (int) ao.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
      }
      int jk = v.get(0) + 1;
      v.set(0, jk);
      while (v.get(0) < 10) {
        String filePathStrin = "src/main/resources/data/" + index1d + v.get(0) + ".ser";
        File fb = new File(filePathStrin);
        if (fb.exists() && !fb.isDirectory()) {
          Bucket bb = Grid.returnbuck(g.grid, v, 0);
          for (ArrayList<Object> aa : bb) {
            int pnum = (int) aa.get(0);
            int tpnum = (int) aa.get(1);
            boolean of = (boolean) aa.get(2);
            int pnumof = (int) aa.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
        int jkk = v.get(0) + 1;
        v.set(0, jkk);
      }
    } else {

    }
    return result;
  }

  public Vector<ArrayList<Object>> execgreateq(SQLTerm sqlTerm) throws DBAppException {
    Vector<ArrayList<Object>> result = new Vector<ArrayList<Object>>();
    Object valu = sqlTerm._objValue;
    String tname = sqlTerm._strTableName;
    String col = sqlTerm._strColumnName;
    int i = getType(tname, col); // int 0 Double 1 String 2 Date 3
    String index1d = tname + col;
    String filePathString = "src/main/resources/data/" + index1d + ".ser";
    File f = new File(filePathString);
    if (f.exists() && !f.isDirectory()) {
      ArrayList<Object> a = new ArrayList<>();
      a.add(valu);
      Grid g = Grid.deserialG(index1d);
      assert g != null;
      Vector<Integer> v = g.returnCell(index1d, a);
      Bucket bj = Grid.returnbuck(g.grid, v, 0);
      if (bj != null) {
        for (ArrayList<Object> ao : bj) {
          Object vall = ao.get(5);
          boolean flag = false;
          if (i == 0) {
            if ((int) vall >= (int) valu) flag = true;
          } else if (i == 1) {
            BigDecimal b = BigDecimal.valueOf((Double) vall);
            BigDecimal b1 = BigDecimal.valueOf((Double) valu);
            int k = b.compareTo(b1);
            if (k >= 0) flag = true;
          } else if (i == 2) {
            int k = ((String) vall).compareTo((String) valu);
            if (k >= 0) flag = true;
          } else {
            int k = ((Date) vall).compareTo((Date) valu);
            if (k >= 0) flag = true;
          }
          if (flag) {
            int pnum = (int) ao.get(0);
            int tpnum = (int) ao.get(1);
            boolean of = (boolean) ao.get(2);
            int pnumof = (int) ao.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
      }
      int jk = v.get(0) + 1;
      v.set(0, jk);
      while (v.get(0) < 10) {
        String filePathStrin = "src/main/resources/data/" + index1d + v.get(0) + ".ser";
        File fb = new File(filePathStrin);
        if (fb.exists() && !fb.isDirectory()) {
          Bucket bb = Grid.returnbuck(g.grid, v, 0);
          for (ArrayList<Object> aa : bb) {
            int pnum = (int) aa.get(0);
            int tpnum = (int) aa.get(1);
            boolean of = (boolean) aa.get(2);
            int pnumof = (int) aa.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
        int jkk = v.get(0) + 1;
        v.set(0, jkk);
      }
    } else {

    }
    return result;
  }

  public Vector<ArrayList<Object>> execlesseq(SQLTerm sqlTerm) throws DBAppException {
    Vector<ArrayList<Object>> result = new Vector<ArrayList<Object>>();
    Object valu = sqlTerm._objValue;
    String tname = sqlTerm._strTableName;
    String col = sqlTerm._strColumnName;
    int i = getType(tname, col); // int 0 Double 1 String 2 Date 3
    String index1d = tname + col;
    String filePathString = "src/main/resources/data/" + index1d + ".ser";
    File f = new File(filePathString);
    if (f.exists() && !f.isDirectory()) {
      ArrayList<Object> a = new ArrayList<>();
      a.add(valu);
      Grid g = Grid.deserialG(index1d);
      assert g != null;
      Vector<Integer> v = g.returnCell(index1d, a);
      Bucket bj = Grid.returnbuck(g.grid, v, 0);
      if (bj != null) {
        for (ArrayList<Object> ao : bj) {
          Object vall = ao.get(5);
          boolean flag = false;
          if (i == 0) {
            if ((int) vall <= (int) valu) flag = true;
          } else if (i == 1) {
            BigDecimal b = BigDecimal.valueOf((Double) vall);
            BigDecimal b1 = BigDecimal.valueOf((Double) valu);
            int k = b.compareTo(b1);
            if (k <= 0) flag = true;
          } else if (i == 2) {
            int k = ((String) vall).compareTo((String) valu);
            if (k <= 0) flag = true;
          } else {
            int k = ((Date) vall).compareTo((Date) valu);
            if (k <= 0) flag = true;
          }
          if (flag) {
            int pnum = (int) ao.get(0);
            int tpnum = (int) ao.get(1);
            boolean of = (boolean) ao.get(2);
            int pnumof = (int) ao.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
      }
      int jk = v.get(0) - 1;
      v.set(0, jk);
      while (v.get(0) > -1) {
        String filePathStrin = "src/main/resources/data/" + index1d + v.get(0) + ".ser";
        File fb = new File(filePathStrin);
        if (fb.exists() && !fb.isDirectory()) {
          Bucket bb = Grid.returnbuck(g.grid, v, 0);
          for (ArrayList<Object> aa : bb) {
            int pnum = (int) aa.get(0);
            int tpnum = (int) aa.get(1);
            boolean of = (boolean) aa.get(2);
            int pnumof = (int) aa.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
        int jkk = v.get(0) - 1;
        v.set(0, jkk);
      }
    } else {

    }
    return result;
  }

  public Vector<ArrayList<Object>> execless(SQLTerm sqlTerm) throws DBAppException {
    Vector<ArrayList<Object>> result = new Vector<ArrayList<Object>>();
    Object valu = sqlTerm._objValue;
    String tname = sqlTerm._strTableName;
    String col = sqlTerm._strColumnName;
    int i = getType(tname, col); // int 0 Double 1 String 2 Date 3
    String index1d = tname + col;
    String filePathString = "src/main/resources/data/" + index1d + ".ser";
    File f = new File(filePathString);
    if (f.exists() && !f.isDirectory()) {
      ArrayList<Object> a = new ArrayList<>();
      a.add(valu);
      Grid g = Grid.deserialG(index1d);
      assert g != null;
      Vector<Integer> v = g.returnCell(index1d, a);
      Bucket bj = Grid.returnbuck(g.grid, v, 0);
      if (bj != null) {
        for (ArrayList<Object> ao : bj) {
          Object vall = ao.get(5);
          boolean flag = false;
          if (i == 0) {
            if ((int) vall < (int) valu) flag = true;
          } else if (i == 1) {
            BigDecimal b = BigDecimal.valueOf((Double) vall);
            BigDecimal b1 = BigDecimal.valueOf((Double) valu);
            int k = b.compareTo(b1);
            if (k < 0) flag = true;
          } else if (i == 2) {
            int k = ((String) vall).compareTo((String) valu);
            if (k < 0) flag = true;
          } else {
            int k = ((Date) vall).compareTo((Date) valu);
            if (k < 0) flag = true;
          }
          if (flag) {
            int pnum = (int) ao.get(0);
            int tpnum = (int) ao.get(1);
            boolean of = (boolean) ao.get(2);
            int pnumof = (int) ao.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
      }
      int jk = v.get(0) - 1;
      v.set(0, jk);
      while (v.get(0) > -1) {
        String filePathStrin = "src/main/resources/data/" + index1d + v.get(0) + ".ser";
        File fb = new File(filePathStrin);
        if (fb.exists() && !fb.isDirectory()) {
          Bucket bb = Grid.returnbuck(g.grid, v, 0);
          for (ArrayList<Object> aa : bb) {
            int pnum = (int) aa.get(0);
            int tpnum = (int) aa.get(1);
            boolean of = (boolean) aa.get(2);
            int pnumof = (int) aa.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
        int jkk = v.get(0) - 1;
        v.set(0, jkk);
      }
    } else {

    }
    return result;
  }

  public Vector<ArrayList<Object>> execnoteq(SQLTerm sqlTerm) throws DBAppException {
    Vector<ArrayList<Object>> result = new Vector<ArrayList<Object>>();
    Object valu = sqlTerm._objValue;
    String tname = sqlTerm._strTableName;
    String col = sqlTerm._strColumnName;
    String cluster = Table.returnCluster(tname);
    int overflowpage = -1;
    if (cluster.equals(col)) {
      boolean flagover = false;
      int[] pos = searchtable(tname, valu);
      int k = -1;
      if (pos[0] >= 0 && pos[1] == -1) {
        Page p = Page.deserialP(tname + pos[0]);
        flagover = true;
        Page overflow = null;
        for (k = 0; k < p.overflow.size(); k++) {
          overflow = p.overflow.get(k);
          if (valu instanceof Integer) {
            pos[1] = binarysearchint(overflow, (int) valu);
          } else if (valu instanceof String) {
            pos[1] = binarysearchstring(overflow, (String) valu);
          } else if (valu instanceof Double) {
            pos[1] = binarysearchdouble(overflow, (double) valu);
          } else if (valu instanceof Date) {
            pos[1] = binarysearchdate(overflow, (Date) valu);
          }
          if (pos[1] >= 0) {
            overflowpage = k;
            break;
          }
        }
      }
      if (pos[0] == -1 || pos[1] == -1) {
        int tablesize = Table.deserialT(tname);
        for (int i = 0; i < tablesize; i++) {
          while (checkdeleted(tname, i)) {
            i++;
            tablesize++;
          }
          Page p = Page.deserialP(tname + i);
          result.addAll(p);
          if (p.overflow.size() != 0) {
            for (int j = 0; j < p.overflow.size(); j++) {
              result.addAll(p.overflow.get(j));
            }
          }
        }

      } else {
        int tablesize = Table.deserialT(tname);
        for (int i = 0; i < tablesize; i++) {
          while (checkdeleted(tname, i)) {
            i++;
            tablesize++;
          }
          if (i == pos[0]) {
            Page p = Page.deserialP(tname + i);
            if (!flagover) {
              for (int j = 0; j < p.size(); j++) {
                if (j != pos[1]) {
                  result.add(p.get(j));
                }
              }
              for (int j = 0; j < p.overflow.size(); j++) {
                result.addAll(p.overflow.get(j));
              }
            } else {
              result.addAll(p);
              for (int j = 0; j < p.overflow.size(); j++) {
                if (j != overflowpage) {
                  result.addAll(p.overflow.get(j));
                } else {
                  for (int l = 0; l < p.overflow.get(j).size(); l++) {
                    if (l != pos[1]) {
                      result.add(p.overflow.get(j).get(l));
                    }
                  }
                }
              }
            }
          } else {
            Page p = Page.deserialP(tname + i);
            result.addAll(p);
            if (p.overflow.size() != 0) {
              for (int j = 0; j < p.overflow.size(); j++) {
                result.addAll(p.overflow.get(j));
              }
            }
          }
        }
      }
    } else { // not equal to the cluster key
      int tablesize = Table.deserialT(tname);
      int A = coloumnnum(col, tname);
      for (int i = 0; i < tablesize; i++) {
        while (checkdeleted(tname, i)) {
          i++;
          tablesize++;
        }
        Page f = Page.deserialP(tname + i);
        int pagesize = f.size();
        int j = 0;
        int m = 0;
        int iverr = f.overflow.size();
        boolean flag = true;
        while (j < pagesize) {
          if (valu instanceof Integer) {
            int int1 = (int) valu;
            int int2 = (int) f.get(j).get(A);
            if (!(int2 == int1)) {
              flag = false;
            }
          } else if (valu instanceof String) {
            String str = (String) valu;
            String Str2 = (String) f.get(j).get(A);
            if (!(Str2.equals(str))) {
              flag = false;
            }
          } else if (valu instanceof Date) {
            Date dt = (Date) valu;
            Date dt2 = (Date) f.get(j).get(A);
            if (!(dt.equals(dt2))) {
              flag = false;
            }
          } else if (valu instanceof Double) {
            String srs = valu.toString();
            Double doo = Double.valueOf(srs);
            BigDecimal big = BigDecimal.valueOf(doo);
            String srs1 = f.get(j).get(A).toString();
            Double doo1 = Double.valueOf(srs1);
            BigDecimal big1 = BigDecimal.valueOf(doo1);
            if (!(big1.equals(big))) {
              flag = false;
            }
          }
          if (flag == false) {
            result.add(f.get(j));
            flag = true;
            j++;
          } else {
            j++;
          }
        }
        while (m < iverr) { // m pages overflow
          int x = 0;
          int oversize = f.overflow.get(m).size();
          boolean flagover = true;
          while (x < oversize) {
            if (valu instanceof Integer) {
              int int1 = (int) valu;
              int int2 = (int) f.overflow.get(m).get(x).get(A);
              if (!(int2 == int1)) {
                flagover = false;
              }
            } else if (valu instanceof String) {
              String str = (String) valu;
              String Str2 = (String) f.overflow.get(m).get(x).get(A);
              if (!(Str2.equals(str))) {
                flagover = false;
              }
            } else if (valu instanceof Date) {
              Date dt = (Date) valu;
              Date dt2 = (Date) f.overflow.get(m).get(x).get(A);
              if (!(dt.equals(dt2))) {
                flagover = false;
              }
            } else if (valu instanceof Double) {
              String srs = valu.toString();
              Double doo = Double.valueOf(srs);
              BigDecimal big = BigDecimal.valueOf(doo);
              String srs1 = f.overflow.get(m).get(x).get(A).toString();
              Double doo1 = Double.valueOf(srs1);
              BigDecimal big1 = BigDecimal.valueOf(doo1);
              if (!(big1.equals(big))) {
                flagover = false;
              }
            }
            if (!flagover) {
              result.add(f.overflow.get(m).get(x));
              flagover = true;
              x++;
            } else {

              x++;
            }
          }
          m++;
        }
      }
    }
    return result;
  }

  public Vector<ArrayList<Object>> execEq(SQLTerm sqlTerm) throws DBAppException {
    Vector<ArrayList<Object>> result = new Vector<ArrayList<Object>>();
    Object valu = sqlTerm._objValue;
    String tname = sqlTerm._strTableName;
    String col = sqlTerm._strColumnName;
    String cluster = Table.returnCluster(tname);
    String index1d = tname + col;
    String filePathString = "src/main/resources/data/" + index1d + ".ser";
    File ff = new File(filePathString);
    if (ff.exists() && !ff.isDirectory()) {
      int i = getType(tname, col);
      ArrayList<Object> a = new ArrayList<>();
      a.add(valu);
      Grid g = Grid.deserialG(index1d);
      assert g != null;
      Vector<Integer> v = g.returnCell(index1d, a);
      Bucket bj = Grid.returnbuck(g.grid, v, 0);
      if (bj != null) {
        for (ArrayList<Object> ao : bj) {
          Object vall = ao.get(5);
          boolean flag = false;
          if (i == 0) {
            if ((int) vall == (int) valu) flag = true;
          } else if (i == 1) {
            BigDecimal b = BigDecimal.valueOf((Double) vall);
            BigDecimal b1 = BigDecimal.valueOf((Double) valu);
            if (b.equals(b1)) flag = true;

          } else if (i == 2) {
            int k = ((String) vall).compareTo((String) valu);
            if (k == 0) flag = true;
          } else {
            int k = ((Date) vall).compareTo((Date) valu);
            if (k == 0) flag = true;
          }
          if (flag) {
            int pnum = (int) ao.get(0);
            int tpnum = (int) ao.get(1);
            boolean of = (boolean) ao.get(2);
            int pnumof = (int) ao.get(3);
            if (of) {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.overflow.get(pnumof).get(tpnum));
            } else {
              Page p = Page.deserialP(tname + pnum);
              result.add(p.get(tpnum));
            }
          }
        }
      } else {
        return result;
      }
    }

    // 1D index
    else if (cluster.equals(col)) {
      boolean flagover = false;
      int[] pos = searchtable(tname, valu);
      int k = -1;
      if (pos[0] >= 0 && pos[1] == -1) {
        Page p = Page.deserialP(tname + pos[0]);
        flagover = true;
        Page overflow = null;

        for (k = 0; k < p.overflow.size(); k++) {
          overflow = p.overflow.get(k);
          if (valu instanceof Integer) {
            pos[1] = binarysearchint(overflow, (int) valu);
          } else if (valu instanceof String) {
            pos[1] = binarysearchstring(overflow, (String) valu);
          } else if (valu instanceof Double) {
            pos[1] = binarysearchdouble(overflow, (double) valu);
          } else if (valu instanceof Date) {
            pos[1] = binarysearchdate(overflow, (Date) valu);
          }
          if (pos[1] >= 0) {
            break;
          }
        }
      }
      if (pos[0] == -1 || pos[1] == -1) {
        return null;
      }
      Page p = Page.deserialP(tname + pos[0]);
      if (!flagover) {
        result.add(p.get(pos[1]));
      } else {
        result.add(p.overflow.get(k).get(pos[1]));
      }
    } else {
      int tablesize = Table.deserialT(tname);
      int A = coloumnnum(col, tname);
      for (int i = 0; i < tablesize; i++) {
        while (checkdeleted(tname, i)) {
          i++;
          tablesize++;
        }
        Page f = Page.deserialP(tname + i);
        int pagesize = f.size();
        int j = 0;
        int m = 0;
        int iverr = f.overflow.size();
        boolean flag = true;
        while (j < pagesize) {
          if (valu instanceof Integer) {
            int int1 = (int) valu;
            int int2 = (int) f.get(j).get(A);
            if (!(int2 == int1)) {
              flag = false;
            }
          } else if (valu instanceof String) {
            String str = (String) valu;
            String Str2 = (String) f.get(j).get(A);
            if (!(Str2.equals(str))) {
              flag = false;
            }
          } else if (valu instanceof Date) {
            Date dt = (Date) valu;
            Date dt2 = (Date) f.get(j).get(A);
            if (!(dt.equals(dt2))) {
              flag = false;
            }
          } else if (valu instanceof Double) {
            String srs = valu.toString();
            Double doo = Double.valueOf(srs);
            BigDecimal big = BigDecimal.valueOf(doo);
            String srs1 = f.get(j).get(A).toString();
            Double doo1 = Double.valueOf(srs1);
            BigDecimal big1 = BigDecimal.valueOf(doo1);
            if (!(big1.equals(big))) {
              flag = false;
            }
          }
          if (flag == true) {
            result.add(f.get(j));
            j++;
          } else {
            flag = true;
            j++;
          }
        }
        while (m < iverr) { // m pages overflow
          int x = 0;
          int oversize = f.overflow.get(m).size();
          boolean flagover = true;
          while (x < oversize) {
            if (valu instanceof Integer) {
              int int1 = (int) valu;
              int int2 = (int) f.overflow.get(m).get(x).get(A);
              if (!(int2 == int1)) {
                flagover = false;
              }
            } else if (valu instanceof String) {
              String str = (String) valu;
              String Str2 = (String) f.overflow.get(m).get(x).get(A);
              if (!(Str2.equals(str))) {
                flagover = false;
              }
            } else if (valu instanceof Date) {
              Date dt = (Date) valu;
              Date dt2 = (Date) f.overflow.get(m).get(x).get(A);
              if (!(dt.equals(dt2))) {
                flagover = false;
              }
            } else if (valu instanceof Double) {
              String srs = valu.toString();
              Double doo = Double.valueOf(srs);
              BigDecimal big = BigDecimal.valueOf(doo);
              String srs1 = f.overflow.get(m).get(x).get(A).toString();
              Double doo1 = Double.valueOf(srs1);
              BigDecimal big1 = BigDecimal.valueOf(doo1);
              if (!(big1.equals(big))) {
                flagover = false;
              }
            }
            if (flagover) {
              result.add(f.overflow.get(m).get(x));
              x++;
            } else {
              flagover = true;
              x++;
            }
          }
          m++;
        }
      }
    }
    return result;
  }

  public static int binarysearchint(Page v1, int key) {
    int low = 0;
    int high = v1.size() - 1;
    int mid = high / 2;
    while (low <= high) {

      int midVal = (int) (v1.get(mid).get(0));

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
      mid = (low + high) / 2;
    }
    return -1; // key not found
  }

  public static int binarysearchstring(Page v1, String key) {
    int low = 0;
    int high = v1.size() - 1;
    int mid = (low + high) / 2;
    while (low <= high) {

      String midVal = (String) (v1.get(mid).get(0));
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
    return -1; // key not found
  }

  public static int binarysearchdate(Page v1, Date key) {
    int low = 0;
    int high = v1.size() - 1;
    int mid = (low + high) / 2;
    while (low <= high) {

      Date midVal = (Date) (v1.get(mid).get(0));
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
    return -1; // key not found
  }

  public static int binarysearchdouble(Page v1, Double key) {
    int low = 0;
    int high = v1.size() - 1;
    int mid = (low + high) / 2;
    while (low <= high) {

      Double midVal = (Double) (v1.get(mid).get(0));
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
    return -1; // key not found
  }

  public static int searchtableall(String tableName, Object key) throws DBAppException {
    int tablesize = Table.deserialT(tableName);
    for (int i = 0; i < tablesize; i++) {
      while (checkdeleted(tableName, i)) {
        i++;
        tablesize++;
      }
      String[] range = returnRange(tableName + i);
      String min = range[0];
      String max = range[1];
      String cluster = Table.returnCluster(tableName);
      int type = DBApp.getType(tableName, cluster); // int 0 Double 1 String 2 Date 3
      if (type == 0) {
        Integer minn = Integer.valueOf(min);
        Integer maxx = Integer.valueOf(max);
        Integer keyy = (Integer) key;
        if (minn <= keyy && keyy <= maxx) {
          return i;
        }
      } else if (type == 2) {
        String keyy = (String) key;
        int compmin = keyy.compareTo(min);
        int compmax = keyy.compareTo(max);
        if (compmin >= 0 && compmax <= 0) {
          return i;
        }
      } else if (type == 3) {
        Date minn;
        try {
          minn = new SimpleDateFormat("yyyy-MM-dd").parse(min);

          Date maxx;
          maxx = new SimpleDateFormat("yyyy-MM-dd").parse(max);

          Date keyy = (Date) key;
          int compmin = keyy.compareTo(minn);
          int compmax = keyy.compareTo(maxx);
          if (compmin >= 0 && compmax <= 0) {
            return i;
          }
        } catch (ParseException e) {
          throw new DBAppException();
        }
      } else if (type == 1) {
        Double minn = Double.valueOf(min);
        Double maxx = Double.valueOf(max);
        Double keyy = (Double) key;
        BigDecimal minbig = BigDecimal.valueOf(minn);
        BigDecimal maxbig = BigDecimal.valueOf(maxx);
        BigDecimal keybig = BigDecimal.valueOf(keyy);
        int compmin = keybig.compareTo(minbig);
        int compmax = keybig.compareTo(maxbig);
        if (compmin >= 0 && compmax <= 0) {
          return i;
        }
      }
    }
    return -1;
  }

  public static int[] searchtable(String t, Object key)
      throws DBAppException { // [page,index inside specified page]
    int[] res = new int[2];
    if (key instanceof Integer) {
      int b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        int b2 = binarysearchint(p1, (int) key);
        res[0] = b1;
        res[1] = b2;
        return res;
      }
    } else if (key instanceof Double) {
      int b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        int b2 = binarysearchdouble(p1, (double) key);
        res[0] = b1;
        res[1] = b2;
        return res;
      }
    } else if (key instanceof Date) {
      int b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        int b2 = binarysearchdate(p1, (Date) key);
        res[0] = b1;
        res[1] = b2;
        return res;
      }
    } else if (key instanceof String) {
      int b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        int b2 = binarysearchstring(p1, (String) key);
        res[0] = b1;
        res[1] = b2;
        return res;
      }
    }
    res[0] = -1;
    res[1] = -1;
    return res;
  }

  public static int coloumnnum(String coloumn, String t1) {
    String[] array = Table.returnColumns(t1);
    for (int i = 0; i < array.length; i++) {
      if (array[i].equals(coloumn)) {
        return i;
      }
    }
    return -1;
  }

  public static boolean insertexist(String t, Object key)
      throws DBAppException { // [page,index inside specified page]
    int[] res = new int[2];
    int b1 = -1;
    int b2 = -1;
    int type = -1;
    if (key instanceof Integer) {
      b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        b2 = binarysearchint(p1, (int) key);
      }
    } else if (key instanceof Double) {
      b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        b2 = binarysearchdouble(p1, (double) key);
      }
    } else if (key instanceof Date) {
      b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        b2 = binarysearchdate(p1, (Date) key);
      }
    } else if (key instanceof String) {
      b1 = searchtableall(t, key);
      if (b1 >= 0) {
        Page p1 = Page.deserialP(t + b1);
        b2 = binarysearchstring(p1, (String) key);
      }
    }
    if (b2 >= 0) {
      return true;
    }
    if (b1 >= 0 && b2 == -1) {
      Page p = Page.deserialP(t + b1);
      Page overflow = null;
      for (int k = 0; k < p.overflow.size(); k++) {
        overflow = p.overflow.get(k);
        if (key instanceof Integer) {
          b2 = binarysearchint(overflow, (int) key);
        } else if (key instanceof String) {
          b2 = binarysearchstring(overflow, (String) key);
        } else if (key instanceof Double) {
          b2 = binarysearchdouble(overflow, (double) key);
        } else if (key instanceof Date) {
          b2 = binarysearchdate(overflow, (Date) key);
        }
        if (b2 >= 0) {
          return true;
        }
      }
    }
    return false;
  }

  public static String[] returnMinMax(String tname, String col) {
    String[] ret = new String[2];
    String line = "";
    String splitBy = ",";
    try {
      BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
      while ((line = br.readLine()) != null) // returns a Boolean value
      {
        String[] row = line.split(splitBy); // use comma as separator
        String line0 = row[0];

        if (line0.equalsIgnoreCase(tname) && row[1].equals(col)) {
          ret[0] = row[5];
          ret[1] = row[6];
          break;
        }
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return ret;
  }

  public static void main(String[] args) throws DBAppException, ParseException {
    DBApp db = new DBApp();
    db.init();

    //    Hashtable htblColNameType = new Hashtable();
    //    htblColNameType.put("id", "java.lang.Integer");
    //    htblColNameType.put("name", "java.lang.String");
    //    htblColNameType.put("gpa", "java.lang.double");
    //    Hashtable htblColNameMin = new Hashtable();
    //    htblColNameMin.put("id", "0");
    //    htblColNameMin.put("name", " ");
    //    htblColNameMin.put("gpa", "0");
    //    Hashtable htblColNameMax = new Hashtable();
    //    htblColNameMax.put("id", "213981");
    //    htblColNameMax.put("name", "ZZZZZZZZZZ");
    //    htblColNameMax.put("gpa", "5");
    //
    //    	db.createTable("trial", "id", htblColNameType, htblColNameMin, htblColNameMax);
    //
    //    Hashtable htblColNameValue = new Hashtable();
    //		 htblColNameValue.put("id", new Integer(5));
    //		 htblColNameValue.put("name", new String("aaaa"));
    //		 htblColNameValue.put("gpa", new Double(2.3));
    //		 db.insertIntoTable("trial",htblColNameValue);
    //
    //		htblColNameValue.clear( );
    //		htblColNameValue.put("id", new Integer(10));
    //		htblColNameValue.put("name", new String("aaaa"));
    //		htblColNameValue.put("gpa", new Double(1.3));
    //		db.insertIntoTable("trial",htblColNameValue);
    //		htblColNameValue.clear( );
    //		htblColNameValue.put("id", new Integer(15));
    //		htblColNameValue.put("name", new String("aaaa"));
    //		htblColNameValue.put("gpa", new Double(2.6));
    //		db.insertIntoTable("trial",htblColNameValue);
    //		htblColNameValue.clear( );
    //		htblColNameValue.put("id", new Integer(20));
    //		htblColNameValue.put("name", new String("aaaa"));
    //		htblColNameValue.put("gpa", new Double(2.0));
    //		db.insertIntoTable("trial",htblColNameValue);
    //		htblColNameValue.clear( );
    //		htblColNameValue.put("id", new Integer(3));
    //		htblColNameValue.put("name", new String("aaaa"));
    //		htblColNameValue.put("gpa", new Double(2.3));
    //		db.insertIntoTable("trial",htblColNameValue);
    //
    //		htblColNameValue.clear( );
    //		htblColNameValue.put("id", new Integer(7));
    //		htblColNameValue.put("name", new String("aaaa"));
    //		htblColNameValue.put("gpa", new Double(2.3));
    //		db.insertIntoTable("trial",htblColNameValue);
    //
    //		htblColNameValue.clear( );
    //		htblColNameValue.put("id", new Integer(2));
    //		htblColNameValue.put("name", new String("aaaa"));
    //		htblColNameValue.put("gpa", new Double(2.0));
    //		db.insertIntoTable("trial",htblColNameValue);
    //
    //    htblColNameValue.clear();
    //    htblColNameValue.put("id", new Integer(1));
    //    htblColNameValue.put("gpa", new Double(2.1));
    //    db.insertIntoTable("trial",htblColNameValue);

    System.out.println(Page.deserialP("trial0"));
    System.out.println(Page.deserialP("trial1"));
    System.out.println((Page.deserialP("trial0")).overflow);
    SQLTerm sql = new SQLTerm();
    sql._objValue = 2.1;
    sql._strColumnName = "gpa";
    sql._strOperator = "<";
    sql._strTableName = "trial";
    Vector<ArrayList<Object>> a = db.execgreateq(sql);
    Vector<ArrayList<Object>> b = db.execEq(sql);
    System.out.println(a);
    System.out.println(b);
    System.out.println(execOperator(a, b, "XOR"));
    //////		Vector<Integer> bucketnumber=new Vector<Integer>();
    //		bucketnumber.add(0);
    //		bucketnumber.add(1);
    //    String[] p = {"gpa"};
    //    db.createIndex("trial",p);
    //   // System.out.println(Arrays.deepToString(Grid.deserialG("trialid").grid));
    // insertintoindex("trialgpaname",bucketnumber,16);
    //    System.out.println(Arrays.deepToString(Grid.deserialG("trialgpaname").grid));
    //    System.out.println(Bucket.deserialB("trialgpaname24"));
    //
    //    System.out.println(Bucket.deserialB("trialgpaname34"));
    //
    //    System.out.println(Bucket.deserialB("trialgpaname40"));
    //
    //    System.out.println(Bucket.deserialB("trialgpaname44"));
    //    System.out.println(Bucket.deserialB("trialgpaname44").overflow);
    //    System.out.println(Bucket.deserialB("trialgpaname54"));
    //    Hashtable<String, Object> columnNameValue=new Hashtable<String,Object>();
    //    columnNameValue.put("gpa",2.0);
    //    //db.updateTablewithindex("trial","5",columnNameValue);
    //    System.out.println(Bucket.deserialB("trialid0"));
    //    System.out.println(Bucket.deserialB("trialid0").overflow);
    //    System.out.println(Page.deserialP("trial0").overflow);
  }
}
