import java.sql.*;

/**
 * @Prigram: PACKAGE_NAME
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-23 13:18
 */
public class ShowJDBC {
    private static String driverName="org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args)throws SQLException {
        //private static String url="jdbc:hive2//192.168.186.111:10000/test";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection(
                "jdbc:hive2://192.168.186.111:10000/test", "root", "123456");

        String tableName = "student";

        String sql = "select * from "+tableName;
        PreparedStatement pstmt;
        try {
            pstmt = con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            int col = rs.getMetaData().getColumnCount();
            System.out.println("============================");
            while (rs.next()) {
                for (int i = 1; i <= col; i++) {
                    System.out.print(rs.getString(i) + "\t");
                    if ((i == 2) && (rs.getString(i).length() < 8)) {
                        System.out.print("\t");
                    }
                }
                System.out.println("");
            }
            System.out.println("============================");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("con:connection failed!");
        }finally{
            try{
                con.close();
            }catch(SQLException e){
                e.printStackTrace();
            }
        }

    }
}
