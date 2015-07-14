
public class Column {
	
	private String columnName;
	private int dataType;
	private String typeName;
	private int columnSize;
	private boolean isPk=false;
	
	public Column(String column_name, int data_type, String type_name, int column_size){
		columnName= column_name;
		dataType= data_type;
		typeName= type_name;
		columnSize= column_size;
	}
	
	public void setPk(boolean value){
		isPk= value;
	}
	
	public String getColname(){
		return columnName;
	}
	
	public int getColtype(){
		return dataType;
	}
	
	public String getColtypeName(){
		return typeName;
	}
	
	public int getColSize(){
		return columnSize;
	}
	
	public boolean isColPK(){
		return isPk;
	}
}
