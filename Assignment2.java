import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2() throws ClassNotFoundException {
	try{
		Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException e){
		e.printStackTrace();
		return;
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException {
	try {
    connection= DriverManager.getConnection(URL, username, password);
	}catch (SQLException e) {
		e.printStackTrace();
		return false;
	}
	if (connection != null) {
		return true;
	} else {
		return false;
	}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() throws SQLException {
    try {
		connection.close();
	} catch (SQLException e) {
		e.printStackTrace();
		return false;  
	}
	return connection.isClosed();
  }
    
  //Inserts row into the winemaker table
  public boolean insertWinemaker(int wmid, String wmname, int cid) throws SQLException {
	String sqlInsert;
	int result;
	//check if cid exists first
	try {
		String sqlCheck= "SELECT cid FROM countries WHERE cid= ?" ;
		ps= connection.prepareStatement(sqlCheck);
		ps.setInt(1, cid);
		rs = ps.executeQuery();

		if (rs.next()) { //country exists
			//the country exists, so now carry out sql query
			sqlInsert= "INSERT INTO winemakers " +
  						"VALUES (?,?,?)";
			PreparedStatement insertwm= connection.prepareStatement(sqlInsert);
			insertwm.setInt(1, wmid);
			insertwm.setString(2, wmname);
			insertwm.setInt(3, cid);
			result= insertwm.executeUpdate();
						
			if (result == 1){  //1 line has been inserted, which is correct
				insertwm.close();
				rs.close(); ps.close();
				return true;
			}else {
   				rs.close(); ps.close();
				insertwm.close();
				return false;
			}	
		} else { //country doesn't exist
			rs.close(); ps.close();
			return false;
		}
	} catch (SQLException e) {
			e.printStackTrace();
			rs.close(); ps.close();
			return false;
	}
  }

    public int getWinemakersCount(String cname) throws SQLException {
	try {

		sql = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		String sqlCount = "SELECT count(WM.wmid) as count FROM winemakers as WM, countries as C "
					+ "WHERE WM.cid = C.cid AND C.cname = " + "'" + cname + "'";
		rs = sql.executeQuery(sqlCount);
		if (rs.next()) {
			int count = rs.getInt("count");
			rs.close();
			sql.close();
			return count;
		} else {
			rs.close(); sql.close();
			return -1;
		}
	} catch (SQLException e) {
		e.printStackTrace();
		rs.close();
		sql.close();
		return -1;
	} //finally {
		//if (sql != null) {sql.close();}
	//}
  }
   public String getWinemakerInfo(int wmid) throws SQLException {
    	try {
    		String sqlInfo= "SELECT W.wmname, C.cname     " +
    				"FROM winemakers W, countries C   " +
    				"WHERE W.wmid= ? AND W.cid= C.cid  " +
    				"ORDER BY W.wmname ASC";
    		ps= connection.prepareStatement(sqlInfo, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    		ps.setInt(1, wmid);		
    		rs= ps.executeQuery();
    				
    		if (!rs.next()){ //resultset is empty
        		rs.close(); //close resultset
        		ps.close(); //close prepared statement
    			return "";  
    		}else {
    			String result = rs.getString(1)+ ":" + rs.getString(2);
    			rs.close(); //close resultset
        		ps.close(); //close prepared statement
    			return result;
    		}

    	} catch (SQLException e){
    		e.printStackTrace();
    		return "";
    	}
      }

    public boolean chgCountry(int cid, String newCName) throws SQLException { 
    	try{
    		String sqlChange= "UPDATE countries	" +
    			   	"SET cname= ? WHERE cid= ?" ;

    		ps= connection.prepareStatement(sqlChange);
    		ps.setString(1, newCName);
    		ps.setInt(2, cid);
    		
    		int result= ps.executeUpdate();
    		ps.close(); 	
			return result >= 0;
    		
    	} catch (SQLException e) {
    		e.printStackTrace();
    		return false;	
    	}
      }

  public boolean deleteCountry(int cid) throws SQLException {
        try{
                //have to delete from 4 different tables otherwise would violate foreign key constraint
                String[] sqlDelete= new String[4]; //create an array of sql Delete queries
                sqlDelete[0]= "DELETE FROM countries     " +
                                                   "WHERE cid= ?" ;
                sqlDelete[1]= "DELETE FROM winemakers    " +
                                                   "WHERE cid= ?" ;
                sqlDelete[2]= "DELETE FROM wine " +
                                                        "WHERE wmid IN (SELECT wmid FROM winemakers WHERE cid= ?)";
                
                sqlDelete[3]= "DELETE FROM pricelist " +
                                                        "WHERE wid IN (SELECT wid FROM wine WHERE wmid IN" +
                                                                        " (SELECT wmid FROM winemakers WHERE cid= ?))";
                //create an array of PreparedStatement objects
                PreparedStatement[] ps= new PreparedStatement[4];
				int[] result= new int[4];
				for (int i=3; i>=0; i--) {
					ps[i]= connection.prepareStatement(sqlDelete[i]);
					ps[i].setInt(1, cid);
					result[i]= ps[i].executeUpdate();	
				}
                return (result[0] >= 0) & (result[1] >= 0) & (result[2] >= 0) & (result[3] >= 0);

        } catch (SQLException e){
                e.printStackTrace();
                return false;   
        }
  }

  public String listWines(int wmid) throws SQLException {
	String result= "";
	try {
		sql = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		String sqlList = "SELECT W.wname, C.cname as country, W.wyear as year, W.bestbeforeny, W.msrp, R.rating "
		+ "FROM winemakers as WM, wine as W, countries as C, ratings as R WHERE R.rid = W.rid AND W.wmid = WM.wmid "
		+ "AND WM.cid = C.cid AND WM.wmid = " + Integer.toString(wmid) + " ORDER BY W.wname ASC, C.cname ASC, W.wyear ASC";
		rs = sql.executeQuery(sqlList);
		while(rs.next()){
			result += rs.getString("wname") + ":" + rs.getString("country") + ":" + rs.getString("year") +
			":" +  rs.getString("bestbeforeny") + ":" +  rs.getString("msrp") + ":" +
			 rs.getString("rating") + "#";
		}
		//remove last "#" from result
		if (result.length() > 0 && result.charAt(result.length()-1)=='#') {
			result = result.substring(0, result.length()-1);
		}
			rs.close();
			sql.close();
			return result;
	} catch (SQLException e) {
		e.printStackTrace();
		return "";
	} finally {
		if(sql != null) sql.close();
	}
  }
public boolean updateRatings(int wmid, int wyear) throws SQLException {
      int rid, rating, wid;
      int up_num=0, count=0;;
      ResultSet new_rid;
      try {
              // find the current rid and rating (<5) of wines produced by winemaker wmid in year wyear
              //rid might be repetitive for some wines so need wid column as well
              String sqlURatings = "SELECT W.wid, W.rid, R.rating FROM wine W, ratings R  " +
                                                      "WHERE W.wmid = ? AND W.rid = R.rid AND W.wyear = ? AND R.rating < 5";
              ps = connection.prepareStatement(sqlURatings, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			  PreparedStatement ps3= connection.prepareStatement("SELECT COUNT(temp.wid) FROM (" + sqlURatings + ") AS temp",
															 	ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
              ps.setInt(1, wmid);
              ps.setInt(2, wyear);
			  ps3.setInt(1, wmid);
			  ps3.setInt(2, wyear);
              rs = ps.executeQuery();
			                
              //check number of updates that need to be carried out
			  ResultSet check= ps3.executeQuery();
              if (check.next()){ //this should only return 1 tuple
                      count= check.getInt(1);
              }
              while (rs.next()) {
                      rid = rs.getInt("rid"); //get rid
                      rating = rs.getInt("rating"); //get current rating
					  wid = rs.getInt("wid");//get wid
                      PreparedStatement ps1= connection.prepareStatement(
                                                              "SELECT rid FROM ratings WHERE rating= ?",
                                                              ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                      ps1.setInt(1, rating+1);
                      new_rid= ps1.executeQuery();
                      if (new_rid.next()) { //if the new rating corresponds to a new rid in ratings table
                              String sqlUpdate= "UPDATE wine SET rid=? WHERE wid=?";
                              PreparedStatement ps2= connection.prepareStatement(sqlUpdate);
                              ps2.setInt(1, new_rid.getInt(1));
                              ps2.setInt(2, wid);
                              up_num += ps2.executeUpdate(); //accumulate the number of updates made
                      }
              }
                      if (up_num == count){
                              sql.close(); 
                              rs.close(); ps.close();
                              return true;
                      } else {
                              sql.close();
                              rs.close(); ps.close();
                              return false;
                      }
      } catch (SQLException e) {
              e.printStackTrace();
              ps.close(); sql.close();
              rs.close();
              return false;
      }
  }
  public String query7() throws SQLException {
		String result = "";
	try{
		//create all views:
		String sqlView1= "CREATE VIEW SWC AS (SELECT DISTINCT WM.wmid, WC.wcname AS color FROM winemakers as WM, wine as W, winecolours as WC, countries as C WHERE WM.wmid = W.wmid AND W.wcid = WC.wcid AND WM.cid = C.cid AND C.cname = 'Spain')";
		Statement view1= connection.createStatement();
		int n= view1.executeUpdate(sqlView1);
		
		String sqlView2= "CREATE VIEW SRandRoseWM AS (SELECT DISTINCT temp.wmid FROM (SELECT SWC1.wmid as wmid, SWC1.color as color1, SWC2.color as color2 FROM SWC as SWC1, SWC as SWC2 WHERE SWC1.wmid = SWC2.wmid AND SWC1.color = 'Red' AND SWC2.color = 'Rose') AS temp)";
		Statement view2= connection.createStatement();
		n= view2.executeUpdate(sqlView2);

		String sqlView3= "CREATE VIEW SRRWMinfo AS (SELECT DISTINCT SRRWM.wmid, C.cid, W.wid, WC.wcid, PL.mid, R.rid, C.cname, PL.startyear, PL.startmonth, PL.endyear, PL.endmonth, PL.price, W.msrp, R.rating FROM SRandRoseWM as SRRWM, wine as W, winemakers as WM, winecolours as WC, countries as C, merchants as M, ratings as R, pricelist as PL WHERE SRRWM.wmid = WM.wmid AND SRRWM.wmid = W.wmid AND WM.cid = C.cid AND R.rid = W.rid AND PL.wid = W.wid AND W.wcid = WC.wcid AND C.cname = 'Spain' AND (WC.wcname = 'Red' OR WC.wcname = 'Rose') AND PL.price < W.msrp AND (((PL.startyear <= 2013 AND 2013 < PL.endyear) OR (2013 = PL.endyear AND 10 <= PL.endmonth)) OR ((PL.endyear >= 2013 AND 2013 > PL.startyear) OR (2013 = PL.startyear AND PL.startyear <= 10))))";
		Statement view3= connection.createStatement();
		n= view3.executeUpdate(sqlView3);

		String sqlView4= "CREATE VIEW SRRWMR AS (SELECT wmid, AVG(distinct rating) as avgRating FROM SRRWMinfo GROUP BY wmid)";
		Statement view4= connection.createStatement();
		n= view4.executeUpdate(sqlView4);

		String sqlView5= "CREATE VIEW temp1 AS (SELECT WM.wmname, SRRWMR.avgRating FROM winemakers as WM, SRRWMR WHERE WM.wmid = SRRWMR.wmid AND SRRWMR.avgRating = (SELECT MAX(SRRWMR1.avgRating) FROM SRRWMR as SRRWMR1))";
		Statement view5= connection.createStatement();
		n= view5.executeUpdate(sqlView5);

		String sqlView6= "CREATE VIEW temp2 AS (SELECT WM.wmname, SRRWMR.avgRating FROM winemakers as WM, SRRWMR WHERE WM.wmid = SRRWMR.wmid AND SRRWMR.avgRating = (SELECT MIN(SRRWMR2.avgRating) FROM SRRWMR as SRRWMR2))";
		Statement view6= connection.createStatement();
		n= view6.executeUpdate(sqlView6);

		String sqlView7= "CREATE VIEW Query7 AS (SELECT * FROM ((select * from temp1) UNION ALL (select * from temp2)) AS temp ORDER BY temp.avgRating DESC, temp.wmname ASC)";
		Statement view7= connection.createStatement();
		n= view7.executeUpdate(sqlView7);

		String sqlQuery7= "SELECT * FROM Query7";
		Statement query7= connection.createStatement();
		rs = query7.executeQuery(sqlQuery7);

		//get result string to return
		while(rs.next()){
			result += rs.getString("wmname") + ":" + rs.getString("avgrating") + "#";
		}
		//remove last "#" from result
		if (result.length() > 0 && result.charAt(result.length()-1)=='#') {
			result = result.substring(0, result.length()-1);
		}
		
		//drop all views before returning;
		String sqlDrop1= "DROP VIEW SWC cascade";
		sql= connection.createStatement();
		int drop = sql.executeUpdate(sqlDrop1);
		view1.close();

		view2.close();

		view3.close();

		view4.close();

		view5.close();

		view6.close();

		view7.close();
		
		query7.close(); sql.close(); rs.close();

		return result;

	} catch (SQLException e) {
		e.printStackTrace();
		return "";
	}
  }
    
  public boolean updateDB() throws SQLException {
		try {
			//create table first
			String sqlCreate= "CREATE TABLE winemakersForAllWines( " +
					   "wmid INTEGER REFERENCES winemakers(wmid) ON DELETE RESTRICT, "+
					   "wmname VARCHAR(20) )";
			sql= connection.createStatement();
			int create= sql.executeUpdate(sqlCreate);
			int count2;
			if (create != 0) return false;

			//fill in table
			String sqlFill= "SELECT Wm.wmid, Wm.wmname " +
					"FROM countries C, winemakers Wm, wine W, winecolours Wc " +
					"WHERE C.cid= Wm.cid AND C.cname='Spain' AND Wm.wmid= W.wmid AND W.wcid= Wc.wcid " +
					"GROUP BY Wm.wmid, Wm.wmname " +
					"HAVING COUNT(DISTINCT Wc.wcname)= 3";

			//create view for this query
			String sqlView= "CREATE VIEW SpainWM AS " + sqlFill;
			Statement view= connection.createStatement();
			int n= view.executeUpdate(sqlView);
		
			//count # rows from this query
			Statement count= connection.createStatement();
			ResultSet rs = count.executeQuery("SELECT COUNT (*) FROM SpainWM");
			

			if(rs.next()){ //this query should only return 1 tuple
				count2= Integer.parseInt(rs.getString(1)); //make the count become integer
			} else {
				view.close(); count.close(); ps.close(); rs.close();
				//drop view before exiting method
				String sqlDropView= "DROP VIEW SpainWM;";
				int drop = sql.executeUpdate(sqlDropView);
				sql.close();
				return false;
			}

			//now insert into table
			String sqlInsert= "INSERT INTO winemakersForAllWines (" + sqlFill + ")" ;
			ps= connection.prepareStatement(sqlInsert);
			int row_num= ps.executeUpdate();
			//check if the right # rows has been inserted
			if (row_num == count2){
				view.close(); count.close(); ps.close(); rs.close();
				String sqlDropView= "DROP VIEW SpainWM;";
				int drop = sql.executeUpdate(sqlDropView);
				sql.close();
				return true;
			} else {
				view.close(); count.close(); ps.close(); rs.close();
				String sqlDropView= "DROP VIEW SpainWM;";
				int drop = sql.executeUpdate(sqlDropView);
				sql.close(); 				
				return false;
			}
			
		} catch (SQLException e){
			e.printStackTrace();
			sql.close(); rs.close(); ps.close();
			return false;	
		}          
	 }
}
