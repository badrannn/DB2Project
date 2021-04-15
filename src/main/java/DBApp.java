import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;



public class DBApp implements DBAppInterface{

	public void init( ) {
		 try (PrintWriter writer = new PrintWriter(new File("metadata.csv"))) {

		    } catch (FileNotFoundException e) {
		      System.out.println(e.getMessage());
		    }
		
	}
	
	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup 
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	// htblColNameMin and htblColNameMax for passing minimum and maximum values
	// for data in the column. Key is the name of the column
	public void createTable(String strTableName,
	String strClusteringKeyColumn,
	Hashtable<String,String> htblColNameType,
	Hashtable<String,String> htblColNameMin,
	Hashtable<String,String> htblColNameMax )
	throws DBAppException{
		try {
			this.appendCsv(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
	}
public void appendCsv(String name,
		String cluster,Hashtable<String,String> htblColNameType,
		Hashtable<String,String> htblColNameMin,
		Hashtable<String,String> htblColNameMax) throws IOException,DBAppException {
	
FileWriter pw =  new FileWriter("metadata.csv",true);
Enumeration<String> type = htblColNameType.keys();

while(type.hasMoreElements()) {
	Boolean clus = false;
	StringBuilder builder = new StringBuilder();
	String col = type.nextElement();
	
	String typ= htblColNameType.get(col);
	String min =htblColNameMin.get(col);
	String max = htblColNameMax.get(col);
	
	if(col.equals(cluster))
		clus = true;
	
	
	builder.append(name+","+col+","+typ+","+clus.toString()+","+"false,"+min+","+max+"\n");
	System.out.println(clus.toString());
	pw.write(builder.toString());
	
	
	
	
	
}
pw.close();
System.out.print("done!");











//String columnNamesList = "Id,name";
//builder.append(columnNamesList +"\n");
//builder.append("1"+",");
//builder.append("opppssoeeee");

	
	
}


	

@Override
public void createIndex(String tableName, String[] columnNames) throws DBAppException {
	// TODO Auto-generated method stub
	
}
@Override
public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
	// TODO Auto-generated method stub
	
}
@Override
public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
		throws DBAppException {
	// TODO Auto-generated method stub
	
}
@Override
public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
	// TODO Auto-generated method stub
	
}
@Override
public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
	// TODO Auto-generated method stub
	return null;
}





@SuppressWarnings("unchecked")
public static void main(String[]args) throws IOException {

	 DBApp db = new DBApp();
	 db.init();
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
	 
	 try {
		db.createTable("Test","id", htblColNameType, htblColNameMin, htblColNameMax);
	} catch (DBAppException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 
	 

}
}

