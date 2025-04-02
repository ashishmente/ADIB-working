import org.apache.camel.Exchange;
import org.apache.log4j.Logger;
import java.sql.*;

public class DatabaseQueryProcessor {

    private static final Logger log = Logger.getLogger("DatabaseQueryProcessor");

    public String process(Exchange exchange) {
        //  Database connection details
			String dbUrl = "jdbc:oracle:thin:@localhost:1521:xe"; // or use @//hostname:port/service_name
			String dbUser = "your_oracle_username";
			String dbPassword = "your_oracle_password";
			String dbDriver = "oracle.jdbc.OracleDriver"; 

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuilder resultString = new StringBuilder();

        try {
            //  Load the database driver
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            log.info("Database connection established successfully!");

            //  Define SQL Query
            String query = "SELECT RIM_NO, BIRTH_DT, CITIZENSHIP, CLASS_CODE, CLASS_CODE_DESCRIPTION, CLOSED_DT, " +
               "COUNTRY_CODE, COUNTRY_OF_BIRTH, CREATE_DT, CREATE_TM, EFFECTIVE_DT, EFFECTIVE_TM, " +
               "EMPL, EMPL_OCCUPATION, EMPL_PHONE, EMPL_POSITION, FIRST_NAME, ID_EXPIRY_DT_1, " +
               "ID_EXPIRY_DT_2, ID_ISSUE_COUNTRY_1, ID_ISSUE_DT_1, ID_ISSUE_DT_2, ID_VALUE_1, ID_VALUE_2, " +
               "LAST_MAINT_DT, LAST_NAME, LAST_SYS_MAINT_DT, MONTH_INCOME, MOTHER_MAIDEN_NAME, NAME_1, " +
               "POSITION_DESCRIPTION, PREV_EMPL, PREV_EMPL_PHONE, RESIDENCE, RIM_TYPE, RISK_CODE, RSM_ID, STATUS " +
               "FROM DELTA_VI_EXT_CUSTOMER_DEMOGRAPHICS WHERE processed = 'N'";
			   
            pstmt = conn.prepareStatement(query);
            rs = pstmt.executeQuery();

            boolean firstRecord = true; // To avoid trailing `#`

            //  Process the result set
            while (rs.next()) {
                if (!firstRecord) {
                    resultString.append("#"); // Append `#` between records
                }
                firstRecord = false;

                resultString.append(rs.getString("BIRTH_DT") != null ? rs.getString("BIRTH_DT") : "").append(",")
                .append(rs.getString("CITIZENSHIP") != null ? rs.getString("CITIZENSHIP") : "").append(",")
                .append(rs.getString("CLASS_CODE") != null ? rs.getString("CLASS_CODE") : "").append(",")
                .append(rs.getString("CLASS_CODE_DESCRIPTION") != null ? rs.getString("CLASS_CODE_DESCRIPTION") : "").append(",")
                .append(rs.getString("CLOSED_DT") != null ? rs.getString("CLOSED_DT") : "").append(",")
                .append(rs.getString("COUNTRY_CODE") != null ? rs.getString("COUNTRY_CODE") : "").append(",")
                .append(rs.getString("COUNTRY_OF_BIRTH") != null ? rs.getString("COUNTRY_OF_BIRTH") : "").append(",")
                .append(rs.getString("CREATE_DT") != null ? rs.getString("CREATE_DT") : "").append(",")
                .append(rs.getString("CREATE_TM") != null ? rs.getString("CREATE_TM") : "").append(",")
                .append(rs.getString("EFFECTIVE_DT") != null ? rs.getString("EFFECTIVE_DT") : "").append(",")
                .append(rs.getString("EFFECTIVE_TM") != null ? rs.getString("EFFECTIVE_TM") : "").append(",")
                .append(rs.getString("EMPL") != null ? rs.getString("EMPL") : "").append(",")
                .append(rs.getString("EMPL_OCCUPATION") != null ? rs.getString("EMPL_OCCUPATION") : "").append(",")
                .append(rs.getString("EMPL_PHONE") != null ? rs.getString("EMPL_PHONE") : "").append(",")
                .append(rs.getString("EMPL_POSITION") != null ? rs.getString("EMPL_POSITION") : "").append(",")
                .append(rs.getString("FIRST_NAME") != null ? rs.getString("FIRST_NAME") : "").append(",")
                .append(rs.getString("ID_EXPIRY_DT_1") != null ? rs.getString("ID_EXPIRY_DT_1") : "").append(",")
                .append(rs.getString("ID_EXPIRY_DT_2") != null ? rs.getString("ID_EXPIRY_DT_2") : "").append(",")
                .append(rs.getString("ID_ISSUE_COUNTRY_1") != null ? rs.getString("ID_ISSUE_COUNTRY_1") : "").append(",")
                .append(rs.getString("ID_ISSUE_DT_1") != null ? rs.getString("ID_ISSUE_DT_1") : "").append(",")
                .append(rs.getString("ID_ISSUE_DT_2") != null ? rs.getString("ID_ISSUE_DT_2") : "").append(",")
                .append(rs.getString("ID_VALUE_1") != null ? rs.getString("ID_VALUE_1") : "").append(",")
                .append(rs.getString("ID_VALUE_2") != null ? rs.getString("ID_VALUE_2") : "").append(",")
                .append(rs.getString("LAST_MAINT_DT") != null ? rs.getString("LAST_MAINT_DT") : "").append(",")
                .append(rs.getString("LAST_NAME") != null ? rs.getString("LAST_NAME") : "").append(",")
                .append(rs.getString("LAST_SYS_MAINT_DT") != null ? rs.getString("LAST_SYS_MAINT_DT") : "").append(",")
                .append(rs.getString("MONTH_INCOME") != null ? rs.getString("MONTH_INCOME") : "").append(",")
                .append(rs.getString("MOTHER_MAIDEN_NAME") != null ? rs.getString("MOTHER_MAIDEN_NAME") : "").append(",")
                .append(rs.getString("NAME_1") != null ? rs.getString("NAME_1") : "").append(",")
                .append(rs.getString("POSITION_DESCRIPTION") != null ? rs.getString("POSITION_DESCRIPTION") : "").append(",")
                .append(rs.getString("PREV_EMPL") != null ? rs.getString("PREV_EMPL") : "").append(",")
                .append(rs.getString("PREV_EMPL_PHONE") != null ? rs.getString("PREV_EMPL_PHONE") : "").append(",")
                .append(rs.getString("RESIDENCE") != null ? rs.getString("RESIDENCE") : "").append(",")
                .append(rs.getString("RIM_TYPE") != null ? rs.getString("RIM_TYPE") : "").append(",")
                .append(rs.getString("RISK_CODE") != null ? rs.getString("RISK_CODE") : "").append(",")
                .append(rs.getString("RSM_ID") != null ? rs.getString("RSM_ID") : "").append(",")
                .append(rs.getString("STATUS") != null ? rs.getString("STATUS") : "");
						
				String RIM_NO = rs.getString("RIM_NO");
                if (RIM_NO != null && !RIM_NO.isEmpty()) { // Ensure RIM_NO is not null or empty
				String updateQuery = "UPDATE apache SET processed = 'Y' WHERE RIM_NO = '" + RIM_NO + "'";
				pstmt = conn.prepareStatement(updateQuery);
				pstmt.executeUpdate();
			}
        }

            //  Log the result
            log.info("Processed Records: " + resultString);

            // Set result in the exchange body
            exchange.getIn().setBody(resultString.toString());

        } catch (Exception e) {
            log.error("Error processing database records: " + e.getMessage(), e);
        } finally {
            //  Close resources
            try {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                log.error("Error closing database resources: " + e.getMessage(), e);
            }
        }
        return resultString.toString();
    }
}
