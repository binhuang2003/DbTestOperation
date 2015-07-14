import java.util.ArrayList;


public class Table {
	private String tableName= null;
	private String owner= null;
	private ArrayList<Column> columnList= new ArrayList<Column>();
	private boolean haspk= false;
	
	public Table(String tablename, String ownername){
		tableName= tablename;
		owner= ownername;
	}
	
	public String getOwner(){
		return owner;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public ArrayList<Column> getColumnList(){
		return columnList;
	}
	
	public void printColumnInfo(){
		for(int i=0; i<columnList.size(); i++){
			Column col= columnList.get(i);
			System.out.println(col.getColname()+", "+col.getColtype()+", "+
			col.getColtypeName()+", "+col.getColSize()+", "+col.isColPK());
			
		}
	}
	
	public void setHasPk(boolean value){
		haspk= value;
	}
	
	public boolean getHasPk(){
		return haspk;
	}
}
