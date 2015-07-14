import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;


public class UsePool extends Thread{
	private ConnectionPool connPool= null;
	
	public UsePool(ConnectionPool value){
		connPool= value;
	}
	
	public void run(){
		int runcount= 0;
		Connection conn;
		try {
			conn = connPool.getConnection();
//			System.out.println("conn="+conn);
			if(conn==null){
				return;
			}
			Operation operation= Operation.getInstance();
			Table table= new Table("T_ORACLE_SYZD_NUMBER500", "HB");
			try {
				operation.GetTableInfo(conn, table);
				while(runcount<500){
					PkObject pkobj= new PkObject();
					try {
						operation.doOperation(conn, table, pkobj);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						System.out.println("Operation has error!");
					}
					
					runcount++;
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.out.println("GetTableInfo has error!");
			}finally{
				conn.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Get connection has error!");
		}
		
		

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ConnectionPool connPool
        = new ConnectionPool("oracle.jdbc.driver.OracleDriver"
                                            ,"jdbc:oracle:thin:@192.168.23.142:1521:gbk10g"
                                            ,"hb"
                                            ,"hb");
		
		try {
			connPool.createPool();
			int thNum= 5;
			UsePool[] upthread= new UsePool[thNum];
			
			for(int i=0; i<thNum; i++){
				upthread[i]= new UsePool(connPool);
				upthread[i].start();
			}
			
			for(int i=0; i<thNum; i++){
				upthread[i].join();
			}
			
			
//			Connection conn = connPool.getConnection();
//			Operation operation= Operation.getInstance();
//			Table table= new Table("T_ORACLE_SYZD_NUMBER500", "HB");
//			operation.GetTableInfo(conn, table);
//			operation.DeleteOperation(conn, table, 4253);
//			operation.UpdateVarcharOp(conn, table, 457903);
//			operation.UpdateTimestampOp(conn, table, 457903);
//			operation.UpdateBlobOp(conn, table, 457903);
//			
////			table.printColumnInfo();
////			operation.InsertOperation(table, 3);
//			int runcount= 0;
//			while(runcount<10000){
//				PkObject pkobj= new PkObject();
//				operation.SelectOperation(table, pkobj);
//				System.out.println("pkvalue="+pkobj.getPkvalue()+"; inserted="+pkobj.getInserted());
//				if(pkobj.getInserted()==false){
//					operation.InsertOperation(table, pkobj.getPkvalue());
//				}else{
//					//update or delete
//				}
//				runcount++;
//			}
			Thread.sleep(5);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("程序运行异常");
		} finally{
			try {
				connPool.closeConnectionPool();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("关闭数据库连接!!");
		}
		
	}

}
