import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class DBApp  implements DBAppInterface{






	public void init( ) {

		try {

			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

			if (br.readLine() == null) {
				System.out.println("No errors, and file empty");
				FileWriter pw =  new FileWriter("src/main/resources/metadata.csv",true);
				pw.write("Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max \n");
				pw.close();
			}

			Path path = Paths.get("src/main/resources/data"); //create data directory
			Files.createDirectories(path);
			File range = new File("src/main/resources/data/range.txt");
			System.out.println("Range file:"+range.createNewFile());

		} catch (IOException e) {

			System.err.println("Failed to create directory!" + e.getMessage());

		}
	}




	public void createTable(String strTableName,
	String strClusteringKeyColumn,
	Hashtable<String,String> htblColNameType,
	Hashtable<String,String> htblColNameMin,
	Hashtable<String,String> htblColNameMax )
	throws DBAppException{
		try {
			if(tableExists(strTableName)){
				throw new DBAppException();
			}
			this.appendCsv(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
			Table t = new Table(strTableName);

			t.cluster = strClusteringKeyColumn;

			t.columns=  new String [htblColNameType.size()];
			t.columns[0]=strClusteringKeyColumn;

			htblColNameType.remove(strClusteringKeyColumn);
			Enumeration<String> keys = htblColNameType.keys();
			for(int i = 1; i<t.columns.length;i++){
				t.columns[i]=keys.nextElement();
			}

			t.serialT();

		} catch (IOException e) {

			e.printStackTrace();
		}
		
	
	}


	public void appendCsv(String name,
		String cluster,Hashtable<String,String> htblColNameType,
		Hashtable<String,String> htblColNameMin,
		Hashtable<String,String> htblColNameMax) throws IOException {
	
		FileWriter pw =  new FileWriter("src/main/resources/metadata.csv",true);
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
	}



	public static boolean tableExists(String name){
		boolean res= false;


		String line = "";
		String splitBy = ",";
		try
		{
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				String line0 = row[0];


				if (line0.equalsIgnoreCase(name)){
					res=true;
					break;
				}

			}

		}
		catch (IOException e)
		{
			e.printStackTrace();

		}


		return res;
	}


	public static boolean checkMinMax(String table, String col, Object key) throws DBAppException {
		int type= getType(table,col); //int 0 Double 1 String 2 Date 3
		String min="";
		String max="";

		String s ="";
		String line = "";
		String splitBy = ",";
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				String line0 = row[0];
				String line1 = row[1];

				if (line0.equalsIgnoreCase(table) && line1.equalsIgnoreCase(col)) {

					min = row[5];
					max=row[6];
					break;
				}

			}
			if (type==0){
				int obj = (int) key;
				try{
					int mi = Integer.parseInt(min);
					int ma = Integer.parseInt(max);
					if(obj<=ma && obj>=mi)
						return true;
					else return false;
				}
				catch (Exception e) {
					throw new DBAppException();

				}
			}
			else if (type==1){
				Double obj = (Double) key;
				BigDecimal obj1=BigDecimal.valueOf(obj);
				try{
					Double mi = Double.parseDouble(min);
					Double ma = Double.parseDouble(max);
					BigDecimal mi1=BigDecimal.valueOf(mi);
					BigDecimal ma1=BigDecimal.valueOf(ma);
					int comp1=obj1.compareTo(mi1);
					int comp2= obj1.compareTo(ma1);


					if(comp1>=0 && comp2<=0)
						return true;                                 
					else return false;
				}
				catch (Exception e) {
					throw new DBAppException();

				}
			}
			else if (type==3){


				try{
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

					Date mi = format.parse(min);
					Date ma = format.parse(max);
					Date obj = (Date)key;

					int comp1=obj.compareTo(mi);
					int comp2=obj.compareTo(ma);

					if(comp1>=0 && comp2<=0)
						return true;
					else return false;
				}
				catch (Exception e) {
					throw new DBAppException();

				}
			}
			else {
				String obj = (String) key;
				int comp1=obj.compareToIgnoreCase(min);
				int comp2=obj.compareToIgnoreCase(max);

				if(comp1>=0 && comp2<=0)
					return true;
				else return false;
			}





		}
		catch (IOException e)
		{
			e.printStackTrace();
			return false;
		}

	}
	public static boolean checkdeleted(String tableName,int pagenum){
		if(returnRange(tableName+pagenum)[0]==null){
		return true;
		}
		return false;
		}

	public static int searchinsert(String tableName,Object key) {//returns page number if value is -1 then insert into first page if -2 then last page
		int tablesize=Table.deserialT(tableName);
		for(int i=0;i<tablesize;i++){
			while(checkdeleted(tableName,i)){
				i++;
				tablesize++;
				}
		String[] range=returnRange(tableName+i);
		String min=range[0];
		String max=range[1];
        String cluster=Table.returnCluster(tableName);
        int type=DBApp.getType(tableName,cluster); //int 0 Double 1 String 2 Date 3
		if(type==0){
        Integer minn=Integer. valueOf(min);
		Integer maxx=Integer. valueOf(max);
		Integer keyy=(Integer) key;
		if(keyy<minn){
			while(checkdeleted(tableName,i-1)){
				if(i==0)
				return -1;
				i--;
				}
			return i-1;
		}
		else if (minn<keyy && keyy<maxx)
			return i;
		}
		else if(type==2){
        String keyy=(String) key;
        int compmin=keyy.compareTo(min);
        int compmax=keyy.compareTo(max);
        if(compmin<0){
			while(checkdeleted(tableName,i-1)){
				if(i==0)
				return -1;
				i--;
				}
			return i-1;
		}
        	
        else if(compmin>0 && compmax<0)
        	return i;
		}
		else if(type==3){

			try {

				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

				Date minn = dateFormat.parse(min);
			
			Date maxx=dateFormat.parse(max);

		Date keyy=(Date) key;
		int compmin=keyy.compareTo(minn);
		int compmax=keyy.compareTo(maxx);
			if(compmin<0){
				while(checkdeleted(tableName,i-1)){
					if(i==0)
					return -1;
					i--;
					}
				return i-1;
			}
			else if(compmin>0 && compmax<0)
				return i;
		}catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		else if(type==1){
			Double minn=Double.valueOf(min);
			Double maxx=Double.valueOf(max);
			Double keyy=(Double)key;
			BigDecimal minbig =BigDecimal.valueOf(minn);
			BigDecimal maxbig =BigDecimal.valueOf(maxx);
			BigDecimal keybig =BigDecimal.valueOf(keyy);
			int compmin=keybig.compareTo(minbig);
			int compmax=keybig.compareTo(maxbig);
			if(compmin<0){
				while(checkdeleted(tableName,i-1)){
					if(i==0)
					return -1;
					i--;
					}
				return i-1;
			}
			else if(compmin>0 && compmax<0)
				return i;
		}
	}
		return -2;
}


	public static int getType(String name,String col){ //int 0 Double 1 String 2 Date 3
		String s ="";
		String line = "";
		String splitBy = ",";
		try
		{
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				String line0 = row[0];
				String line1= row[1];

				if (line0.equalsIgnoreCase(name) && line1.equalsIgnoreCase(col)){

					s=row[2];
					break;
				}

			}
			if (s.equalsIgnoreCase("java.lang.Integer"))
				return 0;
			else if(s.equalsIgnoreCase("java.lang.Double"))
				return 1;
			else if (s.equalsIgnoreCase("java.lang.String"))
				return 2;
			else return 3;




		}
		catch (IOException e)
		{
			e.printStackTrace();
			return -1;
		}
	}


	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
	// TODO Auto-generated method stub
	
	}


	public static String[]returnRange(String pageName){
		String [] ran = new String[2];
		String line = "";
		String splitBy = ",";
		String path ="src/main/resources/data/range.txt";
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(path));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				String line0 = row[0];

				if (line0.equalsIgnoreCase(pageName)){
					ran[0]=row[1];
					ran[1]=row[2];
					break;
				}

			}
			br.close();
			return ran;

		}
		catch (IOException e)
		{
			e.printStackTrace();
			return ran;

		}


	}

	public static void removeBlanks(){
		Scanner file;
		PrintWriter writer;
		String path = "src/main/resources/data/range.txt";
		try {
			file = new Scanner(new File("src/main/resources/data/range.txt")); //sourcefile
			writer = new PrintWriter("tempp.txt"); //destinationfile
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

	public static void pageRecord(String pageName){//update min max
		removeBlanks();
		Page p = Page.deserialP(pageName);
		int lastIndex = p.size()-1;
		Object mini = p.get(0).get(0);
		Object maxi = p.get(lastIndex).get(0);
		String mi;
		String ma;
		if(mini instanceof Date){
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date miniii= (Date) mini;
			Date maxiii= (Date) maxi;
			 mi = dateFormat.format(miniii);
			 ma = dateFormat.format(maxiii);
		}
		else {
			mi = mini.toString();
			ma = maxi.toString();
		}
		String path ="src/main/resources/data/range.txt";
		String temp = "temp.txt";
		File oldfile =new File(path);
		File newfile =new File(temp);
		try {
			String name = "";
			String min = "";
			String max = "";

			String line = "";
			String splitBy = ",";

			FileWriter fw=new FileWriter (temp,true); //start
			BufferedWriter bw= new BufferedWriter(fw);
			PrintWriter pw=new PrintWriter(bw);

			BufferedReader br = new BufferedReader(new FileReader(path));

			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				name=row[0];
				min= row[1];
				max=row[2];
				if(name.equalsIgnoreCase(pageName)) {
					pw.println(name + "," +mi+ "," + ma);
				}
				else{
					pw.println(name + "," + min + "," + max);
				}
			}

			br.close();


			pw.close();

			bw.close();

			fw.close();


			boolean d =oldfile.delete();


			System.out.println("delete status: "+d);

			File dump = new File(path);

			boolean r = newfile.renameTo(dump);

			System.out.println("Rename status: "+r);
		}
		catch (Exception e){
			System.out.println("Error!");

		}




	}
	public  static void deleteRecord( String record){
		removeBlanks();
		String path = "src/main/resources/data/range.txt";
		String temp = "temp.txt";
		File oldfile =new File(path);
		File newfile =new File(temp);
		try {
			String name = "";
			String min = "";
			String max = "";

			String line = "";
			String splitBy = ",";

			FileWriter fw=new FileWriter (temp,true);
			BufferedWriter bw= new BufferedWriter(fw);
			PrintWriter pw=new PrintWriter(bw);

			BufferedReader br = new BufferedReader(new FileReader(path));

			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				name = row[0];
				min = row[1];
				max = row[2];
				if (!name.equalsIgnoreCase(record))
					pw.println(name + "," + min + "," + max);
			}

			br.close();
			pw.close();
			bw.close();
			fw.close();


			boolean d =oldfile.delete();

			System.out.println("delete status: "+d);

			File dump = new File(path);

			boolean r = newfile.renameTo(dump);

			System.out.println("Rename status: "+r);
		}
		catch (Exception e){
			System.out.println("Error!");

		}


	}

	public static boolean checkColumns(String tableName, Hashtable<String, Object> colNameValue){ //true if data consistent
		String [] columns = Table.returnColumns(tableName);
		Enumeration<String> keys = colNameValue.keys();

		while (keys.hasMoreElements()){
			boolean elem = false;
			String key=keys.nextElement();
			for (int i = 0; i <columns.length; i++) {
				if(key.equalsIgnoreCase(columns[i])){
					elem=true;
					break;
				}
			}
			if(elem==false)
				return false;

		}
		return true;

	}
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

		boolean checkTable=tableExists(tableName);
		if (!checkTable){
			throw new DBAppException();
		}
		Comparator<ArrayList<Object>> comparator = new Com();

		ArrayList<Object> row = new ArrayList <>(); //record we wanna insert

		String cluster= Table.returnCluster(tableName); //cluster column
		String[] columns =Table.returnColumns(tableName);



		boolean checkClms = checkColumns(tableName,colNameValue);
		if (checkClms==false){ //false column name was specified (key)
			throw new DBAppException();
		}


		if(!(colNameValue.containsKey(cluster))){ // no cluster key was inserted
			throw  new DBAppException();
		}

		Object valu= colNameValue.get(cluster); //cluster key value
		if(valu==null){ //check if cluster value = null
			throw new DBAppException();
		}

		int[]index = searchtable(tableName,valu);
		//int pageNum= index[0];

		if((index[1]!=-1)){ //check if cluster value = null or cluster value exists already in table
			throw new DBAppException();
		}

			for (int i = 0; i < columns.length; i++) {
				String col = columns[i];
				Object x = colNameValue.get(col);
				int ty = getType(tableName, col); //int 0 Double 1 String 2 Date 3
				if (x == null) {
					row.add(x);
				} else if (((x instanceof Integer) && ty != 0) || ((x instanceof Double) && ty != 1) || ((x instanceof String) && ty != 2) || ((x instanceof Date) && ty != 3)) {
					throw new DBAppException();
				} else {
					boolean check = checkMinMax(tableName, col, x);
					if (!check)
						throw new DBAppException();
					row.add(x);
				}
			}// done with the tuple related exceptions and ready to insert


				//check if table is empty or not
				if(Table.deserialT(tableName)==0){
					try {
						FileWriter pw =  new FileWriter("src/main/resources/data/range.txt",true);
						Page p = new Page();
						StringBuilder builder = new StringBuilder();
						builder.append(tableName+0+","+" "+","+" "+"\n");
						pw.write(builder.toString());
						pw.close();

						p.add(row);
						p.serialP(tableName+0);
						Table.insertInto(tableName);
						pageRecord(tableName+0);

					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				else  {
					int pnum=searchinsert(tableName,valu); //where we should insert the tuple


					if( pnum==-1){ //insert in first page
						int i =0;
						while(checkdeleted(tableName,i)){
							i++;
						}

						Page p = Page.deserialP(tableName+i);
						if(!p.isFull()){
							p.add(row);
							Collections.sort(p,comparator);
							p.serialP(tableName+i);
							pageRecord(tableName+i);
						}
						else { //first check if there's a next page
							if(Table.deserialT(tableName)==1){
								try {
									int j=i+1;
									FileWriter pw =  new FileWriter("src/main/resources/data/range.txt",true);
									StringBuilder builder = new StringBuilder();
									builder.append("\n"+tableName+j+","+"null "+","+" null");
									pw.write(builder.toString());

									pw.close();

									ArrayList removed = p.remove(p.size()-1);
									Page next = new Page();
									next.add(removed);
									p.add(row);

									Collections.sort(p,comparator);



									next.serialP(tableName+j);
									p.serialP(tableName+i);

									pageRecord(tableName+j);
									pageRecord(tableName+i);

									Table.insertInto(tableName);

								} catch (IOException e) {
									e.printStackTrace();
								}

							}
							else {
								int j=i+1;
							while (checkdeleted(tableName,j)){
								j++;
							}

							Page ne =Page.deserialP(tableName+j);

							if(ne.isFull()){
							//overflow
							}
							else {
								ArrayList removed = p.remove(p.size()-1);
								ne.add(removed);
								p.add(row);
								Collections.sort(p,comparator);
								Collections.sort(ne,comparator);
								p.serialP(tableName+i);
								ne.serialP(tableName+j);
								pageRecord(tableName+i);
								pageRecord(tableName+j);
							}
							}


						}

					}

					else if(pnum==-2){
						int i=0;
						int dum = Table.deserialT(tableName);
						while(i<dum){
							if(checkdeleted(tableName,i)){
								i++;
								dum++;
							}else{
								i++;
							}
						}
						i=i-1;
						Page p = Page.deserialP(tableName+i);
						if (p.isFull()){
							try {
								int newPi=i+1;

								FileWriter pw = new FileWriter("src/main/resources/data/range.txt",true);
								StringBuilder builder = new StringBuilder();
								builder.append("\n"+tableName+""+newPi+""+","+"null"+","+"null");
								pw.write(builder.toString());

								pw.close();

								Page newP= new Page();

								newP.add(row);


								Table.insertInto(tableName);

								newP.serialP(tableName+newPi);

								pageRecord(tableName+newPi);

							} catch (IOException e) {
								e.printStackTrace();
							}


						}
						else {
							p.add(row);

							Collections.sort(p,comparator);

							p.serialP(tableName+i);

							pageRecord(tableName+i);

						}

					}

					else{

						Page p = Page.deserialP(tableName+pnum);

						if(!p.isFull()){
							p.add(row);
							Collections.sort(p,comparator);
							p.serialP(tableName+pnum);
							pageRecord(tableName+pnum);
						}
						else {
							int i=0;
							int dum = Table.deserialT(tableName);
							while(i<dum){
								if(checkdeleted(tableName,i)){
									i++;
									dum++;
								}else{
									i++;
								}
							}
							i=i-1;
							if(pnum==i){
								int newPi=i+1;
								try {
									FileWriter pw = new FileWriter("src/main/resources/data/range.txt", true);
									StringBuilder builder = new StringBuilder();
									builder.append("\n" + tableName + "" + newPi + "" + "," + "null" + "," + "null");
									pw.write(builder.toString());

									pw.close();

									Page newP = new Page();
									Table.insertInto(tableName);
									ArrayList removed = p.remove(p.size()-1);

									newP.add(removed);
									p.add(row);
									Collections.sort(p,comparator);
									p.serialP(tableName+pnum);
									newP.serialP(tableName+newPi);
									pageRecord(tableName+pnum);
									pageRecord(tableName+newPi);

								}
								catch (Exception e){
									e.printStackTrace();
								}
							}
							else {
								int j= pnum+1;
								while(checkdeleted(tableName,j)){
									j++;
								}

								Page next = Page.deserialP(tableName+j);

								Vector<ArrayList<Object>> temm= new Vector <>();
								ArrayList r = p.remove(p.size()-1);
								temm.add(row);
								temm.add(r);
								Collections.sort(temm,comparator);

								if(next.isFull()){

								}
								else {

									next.add(temm.get(1));
									p.add(temm.get(0));
									Collections.sort(p,comparator);
									Collections.sort(next,comparator);
									p.serialP(tableName+pnum);
									next.serialP(tableName+j);
									pageRecord(tableName+pnum);
									pageRecord(tableName+j);
								}

							}



						}


					}


				}

	}


	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
		throws DBAppException  {
			boolean check = checkColumns(tableName,columnNameValue);
		if(!check){
			throw new DBAppException();
		}
			String[] columns=Table.returnColumns(tableName);
           int type= getType(tableName, columns[0]);
		   Object clusterkey=-1;
		   if (type==0){
		   clusterkey=Integer.parseInt(clusteringKeyValue);
		   }
		   else if(type==1){
			clusterkey=Double.parseDouble(clusteringKeyValue);
		   }
		   else if(type==3){
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			
					try {
						clusterkey = format.parse(clusteringKeyValue);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			
			
		   }
		

		   else{
			clusterkey=clusteringKeyValue;
		   }
			int[] pos=searchtable(tableName,clusterkey);
			if(pos[0]==-1 || pos[1]==-1){
				throw new DBAppException();
			}
			Enumeration<String> keys = columnNameValue.keys();
			String s=tableName+pos[0];
			Page p=Page.deserialP(s);
			while(keys.hasMoreElements()){
			String key=keys.nextElement();
			int j=coloumnnum(key, tableName);
			if(j==-1){
				throw new DBAppException();
			}
            Object Change = columnNameValue.get(key);
			if(!checkMinMax(tableName, columns[j],Change)){
				throw new DBAppException();
			 }

			
			p.get(pos[1]).set(j, Change);
			
			

	}
	
	p.serialP(s);
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


	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		boolean check = checkColumns(tableName,columnNameValue);
		if(!check){
			throw new DBAppException();
		}
		Enumeration<String> key = columnNameValue.keys();
		boolean flagg=false;
		Object cluster=null;
		while(key.hasMoreElements()){
			String col = key.nextElement();
			if (col.equals( Table.returnCluster(tableName))){
			flagg=true;
			cluster=columnNameValue.get(col);
			break;
			}
		}
		if (flagg) {
			int[] A = searchtable(tableName, cluster);
			if (A[0] == -1 || A[1] == -1) {
				return;
			}
			ArrayList<Object> ele= Page.deserialP(tableName + A[0]).get(A[1]);
			key = columnNameValue.keys();
			boolean flag=true;
			while(key.hasMoreElements()){
				String col = key.nextElement();
					int B = coloumnnum(col, tableName);
					Object value=columnNameValue.get(col);
					if(value instanceof Integer){
						int int1=(int)value;
						int int2=(int)ele.get(B);
					if(!(int2==int1)){
						flag=false;
						break;
					}
					}
					else if(value instanceof String){
						String str=(String) value;
						String Str2=(String) ele.get(B);
						if(!(Str2.equals(str))){
							flag=false;
							break;
					}
				}
					else if(value instanceof Date){
						Date dt=(Date) value;
						Date dt2=(Date) ele.get(B);
						if(!(dt.equals(dt2))){
							flag=false;
							break;

					}
				}
					else if(value instanceof Double){
						String srs=value.toString();
						Double doo=Double.valueOf(srs);
						BigDecimal big = BigDecimal.valueOf(doo);
						String srs1= ele.get(B).toString();
						Double doo1=Double.valueOf(srs1);
						BigDecimal big1 = BigDecimal.valueOf(doo1);
						if(!(big1.equals(big))){
							flag=false;
							break;

					}
				
				}

			}
			if(flag==true){
				Page p=Page.deserialP(tableName+A[0]);
				p.removeElementAt(A[1]);
				if(p.isEmpty()){
					deleteRecord(tableName+A[0]);
					String path="src/main/resources/data/"+tableName+A[0]+".ser";
					File f = new File(path);
					 f.delete();
					 Table.deleteFrom(tableName);
				}
				else{
					p.serialP(tableName+A[0]);
					pageRecord(tableName+A[0]);

				}
			}

		}
		else{
				int tablesize=Table.deserialT(tableName);
            for (int i = 0; i < tablesize; i++) {
				while(checkdeleted(tableName,i)){
					i++;
					tablesize++;
					}
                Page f = Page.deserialP(tableName + i);
				int pagesize=f.size();
				int j=0;
				while( j<pagesize ){
					key = columnNameValue.keys();
					boolean flag=true;
					while (key.hasMoreElements()) {
					String col = key.nextElement();
					int A = coloumnnum(col, tableName);
					Object value=columnNameValue.get(col);
					if(value instanceof Integer){
						int int1=(int)value;
						int int2=(int)f.get(j).get(A);
					if(!(int2==int1)){
						flag=false;
						break;
					}
					}
					else if(value instanceof String){
						String str=(String) value;
						String Str2=(String) f.get(j).get(A);
						if(!(Str2.equals(str))){
							flag=false;
							break;
					}
				}
					else if(value instanceof Date){
						Date dt=(Date) value;
						Date dt2=(Date) f.get(j).get(A);
						if(!(dt.equals(dt2))){
							flag=false;
							break;

					}
				}
					else if(value instanceof Double){
						String srs=value.toString();
						Double doo=Double.valueOf(srs);
						BigDecimal big = BigDecimal.valueOf(doo);
						String srs1= f.get(j).get(A).toString();
						Double doo1=Double.valueOf(srs1);
						BigDecimal big1 = BigDecimal.valueOf(doo1);
						if(!(big1.equals(big))){
							flag=false;
							break;

					}
				
				}

				}
				
				if(flag==true){
					
					f.removeElementAt(j);
                    pagesize=f.size();
					if(f.isEmpty()){
						deleteRecord(tableName+i);
						String path="src/main/resources/data/"+tableName+i+".ser";
						File k = new File(path);
						 k.delete();
						 Table.deleteFrom(tableName);
						 break;
					}
					
				}
				else
				j++;

		}
		if(!f.isEmpty()){
		f.serialP(tableName + i);
		pageRecord(tableName + i);
		}
            }
		}
	}



	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

		return null;
	}

	public static int binarysearchint(Page v1, int key){
		int low = 0;
        int high = v1.size()-1;
		int mid =  high/2;
        while (low <= high) {
            
            int midVal = (int)(v1.get(mid).get(0));

            if (midVal<key)
                low = mid + 1;
            else if (midVal >key)
                high = mid - 1;
            else
                return mid; // key found
			mid = (low + high) /2;
        }
        return - 1;  // key not found
	}

	public static int binarysearchstring(Page v1, String key){
		int low = 0;
        int high = v1.size()-1;
		int mid = (low + high) /2;
        while (low <= high) {
            
            String midVal = (String)(v1.get(mid).get(0));
			int comp=midVal.compareTo(key);

            if (comp<0)
                low = mid + 1;
            else if (comp>0)
                high = mid - 1;
            else
                return mid; // key found
			mid = (low + high) /2;
        }
        return - 1;  // key not found
	}

	public static int binarysearchdate(Page v1, Date key){
		int low = 0;
        int high = v1.size()-1;
		int mid = (low + high) /2;
        while (low <= high) {
            
			Date midVal = (Date)(v1.get(mid).get(0));
			int comp=midVal.compareTo(key);

            if (comp<0)
                low = mid + 1;
            else if (comp>0)
                high = mid - 1;
            else
                return mid; // key found
			mid = (low + high) /2;
        }
        return - 1;  // key not found
	}

	public static int binarysearchdouble(Page v1, Double key){
		int low = 0;
        int high = v1.size()-1;
		int mid = (low + high) /2;
        while (low <= high) {
            
            Double midVal = (Double)(v1.get(mid).get(0));
			BigDecimal comp=BigDecimal.valueOf(midVal);
			BigDecimal key1=BigDecimal.valueOf(key);
			int cond=comp.compareTo(key1);

            if (cond<0)
                low = mid + 1;
            else if (cond==0)
			        return mid; // key found
            else
			high = mid - 1;
                
			mid = (low + high) /2;
        }
        return - 1;  // key not found
	}

	
    public static int searchtableall(String tableName,Object key){
		int tablesize=Table.deserialT(tableName);
		for(int i=0;i<tablesize;i++){
			while(checkdeleted(tableName,i)){
				i++;
				tablesize++;
				}
		String[] range=returnRange(tableName+i);
		String min=range[0];
		String max=range[1];
        String cluster=Table.returnCluster(tableName);
        int type=DBApp.getType(tableName,cluster); //int 0 Double 1 String 2 Date 3
		if(type==0){
        Integer minn=Integer. valueOf(min);
		Integer maxx=Integer. valueOf(max);
		Integer keyy=(Integer) key;
         if (minn<=keyy && keyy<=maxx)
			return i;
		}
		else if(type==2){
        String keyy=(String) key;
        int compmin=keyy.compareTo(min);
        int compmax=keyy.compareTo(max);
        if(compmin>=0 && compmax<=0)
        	return i;
		}
		else if(type==3){
			Date minn;
			try {
				minn = new SimpleDateFormat("yyyy-MM-dd").parse(min);
			
			Date maxx;
				maxx = new SimpleDateFormat("yyyy-MM-dd").parse(max);
			

		Date keyy=(Date) key;
		int compmin=keyy.compareTo(minn);
		int compmax=keyy.compareTo(maxx);
			if(compmin>=0 && compmax<=0)
				return i;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		else if(type==1){
			Double minn=Double.valueOf(min);
			Double maxx=Double.valueOf(max);
			Double keyy=(Double)key;
			BigDecimal minbig =BigDecimal.valueOf(minn);
			BigDecimal maxbig =BigDecimal.valueOf(maxx);
			BigDecimal keybig =BigDecimal.valueOf(keyy);
			int compmin=keybig.compareTo(minbig);
			int compmax=keybig.compareTo(maxbig);
			if(compmin>=0 && compmax<=0)
				return i;
		}
	}
		return -1;

	}
	public static int[] searchtable(String t, Object key)  {//[page,index inside specified page]
	int[] res=new int[2];
	if (key instanceof Integer) {
		int b1=searchtableall(t, key);
		if(b1>=0){
		Page p1=Page.deserialP(t+b1);
		int b2=binarysearchint(p1,(int) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	} }
	else if (key instanceof Double) {
		int b1=searchtableall(t, key);
		if(b1>=0){
		Page p1=Page.deserialP(t+b1);
		int b2=binarysearchdouble(p1,(double) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	}
	}
	else if(key instanceof Date){
		int b1=searchtableall(t, key);
		if(b1>=0){
		Page p1=Page.deserialP(t+b1);
		int b2=binarysearchdate(p1,(Date) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	}
	}
	else if (key instanceof String) {
		int b1=searchtableall(t, key);
		if(b1>=0){
			Page p1=Page.deserialP(t+b1);
			int b2=binarysearchstring(p1,(String) key);
			res[0]=b1;
			res[1]=b2;
			return res ;
		}
	}
	res[0]=-1;
	res[1]=-1;
	return res;

}

	public static int coloumnnum(String coloumn, String t1){
    String[] array=Table.returnColumns(t1);
	for(int i=0;i<array.length;i++){
    if (array[i].equals(coloumn))
		return i;
	
	}
    return -1;
}








	public static void main(String[]args) throws DBAppException, ParseException {
//		DBApp db = new DBApp();
//        db.init();
//
//		deleteRecord("Tat4");
//// 		Hashtable<String,Object> columnNameValue=new Hashtable();
//// columnNameValue.put("id", 6);
//// 		db.deleteFromTable("Tat", columnNameValue);
//// System.out.println(Page.deserialP("Search5"));
//// 		System.out.println(Arrays.toString(searchtable("Search", 40)));
//
//    //   Hashtable htblColNameType = new Hashtable( );
//    //    htblColNameType.put("id","java.lang.Integer");
//    //    htblColNameType.put("name", "java.lang.String");
//    //    htblColNameType.put("gpa", "java.lang.double");
//
//    //    Hashtable htblColNameMin = new Hashtable();
//    //    htblColNameMin.put("id", "0");
//    //    htblColNameMin.put("name", " ");
//    //    htblColNameMin.put("gpa", "0.0");
//
//    //    Hashtable htblColNameMax = new Hashtable();
//    //    htblColNameMax.put("id", "10000");
//    //    htblColNameMax.put("name", "ZZZZZZZZZZ");
//    //    htblColNameMax.put("gpa", "5.0");
//
//
//	// 	db.createTable("Tat", "id", htblColNameType, htblColNameMin, htblColNameMax);
//	// 	Page p= new Page();
//	// 	Page p1= new Page();
//	// 	Page p2= new Page();
//	// 	Page p3= new Page();
//	// 	Page p4= new Page();
//
//	// 	ArrayList<Object> a = new ArrayList <Object>();
//
//	// 	ArrayList<Object> a1 = new ArrayList <Object>();
//
//	// 	ArrayList<Object> a2 = new ArrayList <Object>();
//
//	// 	ArrayList<Object> a3 = new ArrayList <Object>();
//
//	// 	ArrayList<Object> a4 = new ArrayList <Object>();
//	// 	a.add(1);
//	// 	a.add(3.0);
//	// 	a.add("ali");
//
//	// 	a1.add(3);
//	// 	a1.add(2.0);
//	// 	a1.add("asli");
//
//	// 	a2.add(6);
//	// 	a2.add(1.0);
//	// 	a2.add("lie");
//
//	// 	a3.add(8);
//	// 	a3.add(2.4);
//	// 	a3.add("zali");
//
//	// 	a4.add(10);
//	// 	a4.add(2.5);
//	// 	a4.add("zalai");
//	// 	p.add(a);
//	// 	Table.insertInto("Tat");
//
//	// 	p1.add(a1);
//	// 	Table.insertInto("Tat");
//
//	// 	p2.add(a2);
//	// 	Table.insertInto("Tat");
//
//	// 	p3.add(a3);
//	// 	Table.insertInto("Tat");
//
//	// 	p4.add(a4);
//	// 	Table.insertInto("Tat");
//
//
//	// 	p.serialP("Tat0");
//	// 	p1.serialP("Tat1");
//	// 	p2.serialP("Tat2");
//	// 	p3.serialP("Tat3");
//	// 	p4.serialP("Tat4");
//
//// 		t.len=5;
//// 		t.serialT();
////		rename("Test",0);
//
//
//
//<<<<<<< HEAD
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

		// db.createTable("trial", "id", htblColNameType, htblColNameMin, htblColNameMax);

		 Hashtable htblColNameValue = new Hashtable();
//		 htblColNameValue.put("id", new Integer(18));
//		 htblColNameValue.put("name", new String("aaaa"));
//		 htblColNameValue.put("gpa", new Double(2.3));
//		 db.insertIntoTable("trial",htblColNameValue);
//
//		 htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer(15));
//		htblColNameValue.put("name", new String("aaaa"));
//		htblColNameValue.put("gpa", new Double(2.3));
//		db.insertIntoTable("trial",htblColNameValue);
//
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer(13));
//		htblColNameValue.put("name", new String("aaaa"));
//		htblColNameValue.put("gpa", new Double(2.3));
//		db.insertIntoTable("trial",htblColNameValue);
//
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer(10));
//		htblColNameValue.put("name", new String("aaaa"));
//		htblColNameValue.put("gpa", new Double(2.3));
//		db.insertIntoTable("trial",htblColNameValue);

//		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer(20));
		htblColNameValue.put("name", new String("aaaa"));
		htblColNameValue.put("gpa", new Double(2.3));
		//db.insertIntoTable("trial",htblColNameValue);
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer(6));
//		htblColNameValue.put("name", new String("aaaa"));
//		htblColNameValue.put("gpa", new Double(2.3));
//		db.insertIntoTable("trial",htblColNameValue);


//		Hashtable<String,Object> columnNameValue=new Hashtable();
//		columnNameValue.put("id", 99);
//		db.deleteFromTable("trial", columnNameValue);
	//Table.insertInto("trial");
		System.out.println(Table.deserialT("trial"));
//		System.out.println(Page.deserialP("trial0"));
//		System.out.println(Page.deserialP("trial1"));
		System.out.println(Page.deserialP("trial2"));
		System.out.println(Page.deserialP("trial3"));


//=======
		//  DBApp db = new DBApp();
		//  db.init();
////		 Hashtable htblColNameType = new Hashtable( );
////		 htblColNameType.put("id", "java.lang.Integer");
////		 htblColNameType.put("name", "java.lang.String");
////		 htblColNameType.put("gpa", "java.lang.double");
////		 Hashtable htblColNameMin = new Hashtable();
////		 htblColNameMin.put("id", "0");
////		 htblColNameMin.put("name", " ");
////		 htblColNameMin.put("gpa", "0");
////		 Hashtable htblColNameMax = new Hashtable();
////		 htblColNameMax.put("id", "213981");
////		 htblColNameMax.put("name", "ZZZZZZZZZZ");
////		 htblColNameMax.put("gpa", "5");
////
////		 db.createTable("Test", "id", htblColNameType, htblColNameMin, htblColNameMax);
//		//  Hashtable htblColNameValue = new Hashtable();
//		//  htblColNameValue.put("id", new Integer(240));
//		//  htblColNameValue.put("name", new String("aaaa"));
//		//  htblColNameValue.put("gpa", new Double(3));
//		// htblColNameValue.put("gpaa", new Double(3));
//
//		// System.out.println(checkColumns("Test",htblColNameValue));
//>>>>>>> fd0611ae0b5fc5d735b46988ddcfabf77596e35c
//
////		 htblColNameValue.clear( );
////		// htblColNameValue.put("id", new Integer( 290 ));
//		// htblColNameValue.put("name", new String("ali" ) );
//		// htblColNameValue.put("gpa", new Double( 0.95 ) );
//		//Table.insertInto("Test");
//		//db.insertIntoTable( "Test" , htblColNameValue );
//		// System.out.println(Page.deserialP("Test1"));
//		//System.out.println(Page.deserialP("Test0"));
//
//// 				Table t = new Table("Test");
//
////            t.cluster = "id";
//
//
//
//// 		// editRecord("src/main/resources/data/range.txt","abdo","zozo","fofo");
//
////         //    t.columns = new String[htblColNameType.size()];
////         //    t.columns[0] = "id";
//
////         //    htblColNameType.remove("id");
////         //    Enumeration<String> keys = htblColNameType.keys();
////         //    for (int i = 1; i < t.columns.length; i++) {
////         //        t.columns[i] = keys.nextElement();
////         //    }
//
////         //    t.serialT();
//
////        Page Test0 = new Page();
//
////        ArrayList<Object> a4 = new ArrayList<>();
////        ArrayList<Object> a5 = new ArrayList<>();
//// 		ArrayList<Object> a6 = new ArrayList<>();
//
////        a4.add(1);
////        a4.add(2.0);
//// 		a4.add("mus");
//
////        a5.add(2);
////        a5.add(2.0);
//// 		a5.add("mus");
//// //
//// 		Page Test1 = new Page();
//// //
//// //
////        a6.add(4);
////        a6.add(2.0);
//// 		a6.add("soooo");
//
//// 		Page Test2 = new Page();
//
////        Test0.add(a4);
////         Table.insertInto("Test");
////        Test1.add(a5);
//
//// 		Table.insertInto("Test");
//
////        Test2.add(a6);
//// 		Test0.serialP("Test0");
//// 		Test1.serialP("Test1");
//// 		Test2.serialP("Test2");
//// pageRecord("String0");
//// pageRecord("String1");
//// pageRecord("String2");
//// pageRecord("String3");
//// pageRecord("String4");
////int v=searchinsert("Double", 10.0);
////		System.out.println(v);v
////Table.insertInto("Test");
//// Hashtable<String,Object> columnNameValue=new Hashtable();
//// columnNameValue.put("id", 1);
//
//// db.deleteFromTable("Test", columnNameValue);
////System.out.print(Page.deserialP("Test1"));
////		Table.insertInto("Test");
//<<<<<<< HEAD
//	//	deleteRecord("baba");
//=======
//		// deleteRecord("baba");
//>>>>>>> fd0611ae0b5fc5d735b46988ddcfabf77596e35c
////
////
////       t.add(Test0);
////		t.add(Test1);
////		t.add(Test2);
////		Test0.serialP("Test0");
////		Test1.serialP("Test1");
////		Test2.serialP("Test2");
////        t.serialT();
////		//rename("Test", 1);
////        //Hashtable<String,Object> hi=new Hashtable<String,Object>();
////        //hi.put("gpa", 2.0);
////		//hi.put("name", "mus");
////
////
////
////
////        //db.deleteFromTable("Test",hi);
////		//System.out.print(Page.deserialP("Test0"));
//

 	}

}