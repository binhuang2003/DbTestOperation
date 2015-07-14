import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.sql.Types;
import java.text.SimpleDateFormat;

public class Operation {
//	private Connection conn;
	private static long MAX=10000;
	private static long MIN=1;
	private static Operation operation= new Operation();
	public static Operation getInstance(){
		return operation;
	}
	
	public void GetTableInfo(Connection conn, Table table) throws Exception{
		DatabaseMetaData dbmd= null;
		try {
			dbmd = conn.getMetaData();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		
		ArrayList<Column> columnlist= table.getColumnList();
		ResultSet rs = dbmd.getColumns(conn.getCatalog(), table.getOwner(), table.getTableName(), null);
		while(rs.next()){
//			System.out.print("name="+rs.getString("COLUMN_NAME"));
//			System.out.print(", type="+rs.getString("DATA_TYPE"));
//			System.out.print(", type_name="+rs.getString("TYPE_NAME"));
//			System.out.print(", size="+rs.getInt("COLUMN_SIZE"));
//			System.out.println();
			int type= rs.getInt("DATA_TYPE");
			String typeName= rs.getString("TYPE_NAME");
			if(type==1111 && typeName.equals("NVARCHAR2")){
				type=Types.VARCHAR;
			}else if(type==1111 && typeName.equals("NCLOB")){
				type= Types.CLOB;
			}else if(type==-3 && typeName.equals("RAW")){
				type= Types.VARBINARY;
			}else if(type==-4 && typeName.equals("LONG RAW")){
				type= Types.LONGVARBINARY;
			}
			Column col= new Column(rs.getString("COLUMN_NAME"), type,
					typeName, rs.getInt("COLUMN_SIZE"));
			columnlist.add(col);
		}
		rs.close();
		ResultSet pkRSet = dbmd.getPrimaryKeys(conn.getCatalog(), table.getOwner(), table.getTableName());
		while(pkRSet.next()){
//			System.out.print("pk_name="+pkRSet.getString("COLUMN_NAME"));
//			System.out.println();
			for(int i=0;i<columnlist.size();i++){
				Column col= columnlist.get(i);
				if(pkRSet.getString("COLUMN_NAME").equals(col.getColname())){
					col.setPk(true);
				}
			}
			table.setHasPk(true);
		}
		pkRSet.close();
	}
	
	public String CreateInsertSQL(Table table){
		StringBuffer strSQL= new StringBuffer();
		strSQL.append("INSERT INTO "+table.getOwner()+"."+table.getTableName()+" (");
		ArrayList<Column> columnList= table.getColumnList();
		for(int i=0; i<columnList.size(); i++){
			if(i!=0){
				strSQL.append(",");
			}
			strSQL.append("\""+columnList.get(i).getColname()+"\"");
		}
		strSQL.append(") VALUES (");
		for(int i=0; i<columnList.size(); i++){
			if(i!=0){
				strSQL.append(",");
			}
			strSQL.append("?");
		}
		strSQL.append(")");
		return strSQL.toString();
	}
	
	public String CreateUpdateSQL(String updatecol, String owner, String tbName, String pkcol, long pk){
		String str="";
		str += "UPDATE ";
		str += owner+"."+tbName;
		str += " SET ";
		str += updatecol+"=?";
		str += " WHERE "+pkcol+"="+String.valueOf(pk);
		
		return str;
	}
	
	public String CreateDeleteSQL(String owner, String tbName, String pkcol, long pk){
		String str="";
		str += "DELETE FROM ";
		str += owner+"."+tbName;
		str += " WHERE "+pkcol+"="+String.valueOf(pk);
		
		return str;
	}
	
	public int UpdateVarcharOp(Connection conn, Table table, long pk) throws Exception{
		int ret= 0;
		long rdvalue= Math.round(Math.random()*(MAX-MIN)+MIN);
		ArrayList<Column> columnList= table.getColumnList();
		String varcharCol="";
		String pkCol="";
		String sql="";
		Column col=null;
		Column pkcol=null;
		try{
			for(int i=0; i<columnList.size(); i++){
				col= columnList.get(i);
				if(col.getColtype()==Types.VARCHAR && !col.isColPK()){
					varcharCol= col.getColname();
					break;
				}
			}
			for(int i=0; i<columnList.size(); i++){
				pkcol= columnList.get(i);
				if(pkcol.isColPK()){
					pkCol= pkcol.getColname();
					break;
				}
			}
			if(varcharCol.length()>0 && pkCol.length()>0 && col!=null){
				sql= this.CreateUpdateSQL(varcharCol, table.getOwner(), table.getTableName(), pkCol, pk);
				System.out.println("sql="+sql);
				PreparedStatement pstmt1= conn.prepareStatement(sql);
				oracle.jdbc.OraclePreparedStatement pstmt= (oracle.jdbc.OraclePreparedStatement)pstmt1;
				String varcharval= "VARCHAR如何获取正在运行的线程的ID？"+rdvalue;
				if(col.getColtypeName().equals("NVARCHAR2") || col.getColtypeName().equals("NCHAR")){
					pstmt.setFormOfUse(1, oracle.jdbc.OraclePreparedStatement.FORM_NCHAR);
				}
				pstmt.setString(1, varcharval);
				pstmt.executeUpdate();
				pstmt.close();
				conn.commit();
			}else{
				return ret;
			}
		}catch(Exception e){
			throw e;
		}
		return ret;
	}
	
	public int UpdateTimestampOp(Connection conn, Table table, long pk) throws Exception{
		int ret= 0;
		long rdvalue= Math.round(Math.random()*(MAX-MIN)+MIN);
		ArrayList<Column> columnList= table.getColumnList();
		String varcharCol="";
		String pkCol="";
		String sql="";
		Column col=null;
		Column pkcol=null;
		try{
			for(int i=0; i<columnList.size(); i++){
				col= columnList.get(i);
				if(col.getColtype()==Types.TIMESTAMP && !col.isColPK()){
					varcharCol= col.getColname();
					break;
				}
			}
			for(int i=0; i<columnList.size(); i++){
				pkcol= columnList.get(i);
				if(pkcol.isColPK()){
					pkCol= pkcol.getColname();
					break;
				}
			}
			if(varcharCol.length()>0 && pkCol.length()>0 && col!=null){
				sql= this.CreateUpdateSQL(varcharCol, table.getOwner(), table.getTableName(), pkCol, pk);
				System.out.println("sql="+sql);
				PreparedStatement pstmt= conn.prepareStatement(sql);
				String dateVal= "2015-07-14 15:44:35";
				pstmt.setTimestamp(1, Timestamp.valueOf(dateVal));
				pstmt.executeUpdate();
				pstmt.close();
				conn.commit();
			}else{
				return ret;
			}
		}catch(Exception e){
			throw e;
		}
		return ret;
	}
	
	public int UpdateClobOp(Connection conn, Table table, long pk) throws Exception{
		int ret= 0;
		ArrayList<Column> columnList= table.getColumnList();
		String varcharCol="";
		String pkCol="";
		String sql="";
		Column col=null;
		Column pkcol=null;
		try{
			for(int i=0; i<columnList.size(); i++){
				col= columnList.get(i);
				if(col.getColtype()==Types.CLOB && !col.isColPK()){
					varcharCol= col.getColname();
					break;
				}
			}
			for(int i=0; i<columnList.size(); i++){
				pkcol= columnList.get(i);
				if(pkcol.isColPK()){
					pkCol= pkcol.getColname();
					break;
				}
			}
			if(varcharCol.length()>0 && pkCol.length()>0 && col!=null){
				sql= this.CreateUpdateSQL(varcharCol, table.getOwner(), table.getTableName(), pkCol, pk);
				System.out.println("sql="+sql);
				PreparedStatement pstmt1= conn.prepareStatement(sql);
				oracle.jdbc.OraclePreparedStatement pstmt= (oracle.jdbc.OraclePreparedStatement)pstmt1;
				File file= new File("D:\\zty.txt");
				if(file.exists()){
//					BufferedReader br= new BufferedReader(new InputStreamReader(new FileInputStream(file)));
//					String data= null;
//					while((data=br.readLine())!=null){
//						System.out.println(data);
//					}
//					br.close();
//					InputStreamReader reader= new InputStreamReader(new FileInputStream(file), "UTF-8");
					Reader reader= new FileReader(file);
					int len= (int)file.length();
//					System.out.println("file.length="+file.length());
//					String str1= "sssss保险勘探现场的时候会有一张现场单,你拿去4S,其他他们会搞定";
//					Reader reader  = new StringReader(str1);
					try{
						pstmt.setCharacterStream(1, reader, len);//len只能转换为int，才能执行成功。
					}catch(SQLException e){
						e.printStackTrace();
					} 
					pstmt.executeUpdate();
					reader.close();
				}
				
				pstmt.close();
				conn.commit();
			}else{
				return ret;
			}
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		return ret;
	}
	
	public int UpdateBlobOp(Connection conn, Table table, long pk) throws Exception{
		int ret= 0;
		ArrayList<Column> columnList= table.getColumnList();
		String varcharCol="";
		String pkCol="";
		String sql="";
		Column col=null;
		Column pkcol=null;
		try{
			for(int i=0; i<columnList.size(); i++){
				col= columnList.get(i);
				if(col.getColtype()==Types.BLOB && !col.isColPK()){
					varcharCol= col.getColname();
					break;
				}
			}
			for(int i=0; i<columnList.size(); i++){
				pkcol= columnList.get(i);
				if(pkcol.isColPK()){
					pkCol= pkcol.getColname();
					break;
				}
			}
			if(varcharCol.length()>0 && pkCol.length()>0 && col!=null){
				sql= this.CreateUpdateSQL(varcharCol, table.getOwner(), table.getTableName(), pkCol, pk);
				System.out.println("sql="+sql);
				PreparedStatement pstmt1= conn.prepareStatement(sql);
				oracle.jdbc.OraclePreparedStatement pstmt= (oracle.jdbc.OraclePreparedStatement)pstmt1;
				File file= new File("D:\\sql.jpg");
				if(file.exists()){
					InputStream in= new FileInputStream(file);
					int len= (int)file.length();
					pstmt.setBinaryStream(1, in, len);//len只能转换为int，才能执行成功。
					pstmt.executeUpdate();
					in.close();
				}
				
				pstmt.close();
				conn.commit();
			}else{
				return ret;
			}
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		return ret;
	}
	
	public int DeleteOperation(Connection conn, Table table, long pk) throws Exception{
		int ret= 0;
		ArrayList<Column> columnList= table.getColumnList();
		String pkCol="";
		String sql="";
		Column pkcol=null;
		try{
			for(int i=0; i<columnList.size(); i++){
				pkcol= columnList.get(i);
				if(pkcol.isColPK()){
					pkCol= pkcol.getColname();
					break;
				}
			}
			if(pkCol.length()>0 && pkcol!=null){
				sql= this.CreateDeleteSQL(table.getOwner(), table.getTableName(), pkCol, pk);
				System.out.println("sql="+sql);
				PreparedStatement pstmt= conn.prepareStatement(sql);
				pstmt.executeUpdate();
				pstmt.close();
				conn.commit();
			}else{
				return ret;
			}
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		return ret;
	}
	
	private void SetPreparedStatement(PreparedStatement pstmt1, Table table, long pkValue) throws SQLException{
		ArrayList<Column> columnList= table.getColumnList();
		int size= columnList.size();
		int index= 0;
		oracle.jdbc.OraclePreparedStatement pstmt= (oracle.jdbc.OraclePreparedStatement)pstmt1;
		long rdvalue= Math.round(Math.random()*(MAX-MIN)+MIN);
		int setIndex= 0;
		try{
			for(index=0; index<size; index++){
				Column col= columnList.get(index);
				setIndex= index+1;
				switch(col.getColtype()){
				case Types.DECIMAL://NUMBER
					if(col.isColPK()){
						pstmt.setBigDecimal(setIndex, BigDecimal.valueOf(pkValue));
//						pstmt.setLong(setIndex, Long.valueOf(pkValue));
					}else{
						pstmt.setBigDecimal(setIndex, BigDecimal.valueOf(rdvalue));
					}
					break;
				case Types.CHAR:
					String charval= "CHAR出了事故不要害怕,立刻打110和保险公司"+String.valueOf(rdvalue);
					pstmt.setString(setIndex, charval);
					break;
				case Types.VARCHAR:
					String varcharval= "VARCHAR保险公司就请非常专业的讼棍帮你打官司"+String.valueOf(rdvalue);
					if(col.getColtypeName().equals("NVARCHAR2") || col.getColtypeName().equals("NCHAR")){
						pstmt.setFormOfUse(setIndex, oracle.jdbc.OraclePreparedStatement.FORM_NCHAR);
					}
					pstmt.setString(setIndex, varcharval);
					break;
				case Types.DATE:
				case Types.TIME:
				case Types.TIMESTAMP:
					String dateVal= "2015-07-13 14:28:35";
					SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:MM:SS");
					pstmt.setTimestamp(setIndex, Timestamp.valueOf(dateVal));
					break;
				case Types.CLOB:
					String str1= "保险勘探现场的时候会有一张现场单,你拿去4S,其他他们会搞定";
					Reader reader  = new StringReader(str1);
					pstmt.setCharacterStream(setIndex, reader, str1.length());
					break;
				case Types.BLOB:
				case Types.LONGVARBINARY:
					String str2= "任何关于医院的费用你说我没钱,请和我的保险公司联系.";
					InputStream is = new ByteArrayInputStream(str2.getBytes());
					pstmt.setBinaryStream(setIndex, is, str2.getBytes().length);
					break;
				case Types.VARBINARY:
					String str3= "任何关于";
					InputStream is1 = new ByteArrayInputStream(str3.getBytes());
					pstmt.setBinaryStream(setIndex, is1, str3.getBytes().length);
					break;
				default:
					break;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
	}
	
	public void InsertOperation(Connection conn, Table table, long pk) throws Exception{
		String strsql= this.CreateInsertSQL(table);
		
		try {
			PreparedStatement pstmt= conn.prepareStatement(strsql);
			SetPreparedStatement(pstmt, table, pk);
			pstmt.executeUpdate();
			pstmt.close();
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	public long SelectOperation(Connection conn, Table table, PkObject pkobj) throws Exception{
		long rdvalue= Math.round(Math.random()*(MAX-MIN)+MIN);
//		rdvalue= 3;
		pkobj.setInserted(false);
		pkobj.setPkvalue(rdvalue);
		String strsql= "select count(";
		String strPKname= "";
		if(table.getHasPk()){
			ArrayList<Column> columnlist= table.getColumnList();
			for(int i=0;i<columnlist.size();i++){
				Column col= columnlist.get(i);
				if(col.isColPK()){
					strsql += col.getColname();
					strPKname= col.getColname();
					break;
				}
			}
			strsql += ") from ";
			strsql += table.getOwner();
			strsql += ".";
			strsql += table.getTableName();
			strsql += " where ";
			strsql += strPKname;
			strsql += "=";
			strsql += String.valueOf(rdvalue);
		}else{
			return -1;
		}
		try {
//			System.out.println("select sql="+strsql);
			PreparedStatement pstmt= conn.prepareStatement(strsql);
			ResultSet rs=pstmt.executeQuery();
			int count= 0;
			while(rs.next()){
				count= rs.getInt(1);
			}
			if(count>0)
				pkobj.setInserted(true);
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return rdvalue;
	}
	
	public synchronized void doOperation(Connection conn, Table table, PkObject pkobj) throws Exception{
		try {
			this.SelectOperation(conn, table, pkobj);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception("select has error:"+e.getMessage());
		}
		
		
		try {
			if(pkobj.getInserted()==false){
				System.out.println("pkvalue="+pkobj.getPkvalue()+"; inserte"+"; thread="+Thread.currentThread().getId());
				this.InsertOperation(conn, table, pkobj.getPkvalue());
			}else{
				//update or delete
				MathRandom mr= new MathRandom();
				int rate= mr.PercentageRandom();
				if(rate==0){
					System.out.println("pkvalue="+pkobj.getPkvalue()+"; update varchar"+"; thread="+Thread.currentThread().getId());
					this.UpdateVarcharOp(conn, table, pkobj.getPkvalue());
				}else if(rate==1){
					System.out.println("pkvalue="+pkobj.getPkvalue()+"; update clob"+"; thread="+Thread.currentThread().getId());
					this.UpdateClobOp(conn, table, pkobj.getPkvalue());
				}else if(rate==2){
					System.out.println("pkvalue="+pkobj.getPkvalue()+"; update blob"+"; thread="+Thread.currentThread().getId());
					this.UpdateBlobOp(conn, table, pkobj.getPkvalue());
				}else if(rate==3){
					System.out.println("pkvalue="+pkobj.getPkvalue()+"; update ts"+"; thread="+Thread.currentThread().getId());
					this.UpdateTimestampOp(conn, table, pkobj.getPkvalue());
				}else{
					System.out.println("pkvalue="+pkobj.getPkvalue()+"; delete"+"; thread="+Thread.currentThread().getId());
					this.DeleteOperation(conn, table, pkobj.getPkvalue());
				}
			}
		} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new Exception("operation has error:"+e.getMessage());
		}
		
	}
}
