
public class PkObject {
	private long pkvalue;
	private boolean inserted;
	public PkObject(){
		
	}
	public void setPkvalue(long value){
		pkvalue= value;
	}
	public long getPkvalue(){
		return pkvalue;
	}
	public void setInserted(boolean value){
		inserted= value;
	}
	public boolean getInserted(){
		return inserted;
	}
}
