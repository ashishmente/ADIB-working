import org.apache.camel.Exchange;
import org.apache.log4j.Logger;
import java.sql.*;

public class DatabaseQueryProcessor {

    private static final Logger log = Logger.getLogger(DatabaseQueryProcessor.class);

    public void process(Exchange exchange) {
        // Database connection details
        String dbUrl = "jdbc:oracle:thin:@localhost:1521:xe"; // or use @//hostname:port/service_name
		String dbUser = "your_oracle_username";
		String dbPassword = "your_oracle_password";
		String dbDriver = "oracle.jdbc.OracleDriver"; 

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
	
        try {
            // Load the database driver
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            log.info("Database connection established successfully!");

            // Define SQL Query to fetch only the first record
            String query = "SELECT processed FROM DELTA_VI_EXT_CUSTOMER_DEMOGRAPHICS ORDER BY id LIMIT 1";
            pstmt = conn.prepareStatement(query);
            rs = pstmt.executeQuery();

            if (rs.next()) {
                String processedStatus = rs.getString("processed"); // Get processed column value
                log.info("First row processed status: " + processedStatus);

                // Set header in the exchange based on processed value
                if ("Y".equalsIgnoreCase(processedStatus)) {
                    exchange.getIn().setHeader("IsProcessed", "YES");
                } else {
                    exchange.getIn().setHeader("IsProcessed", "NO");
                }
            } else {
                log.info("No records found in the table.");
                exchange.getIn().setHeader("IsProcessed", "UNKNOWN");
            }

        } catch (Exception e) {
            log.error("Error processing database records: " + e.getMessage(), e);
        } finally {
            // Close resources
            try {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                log.error("Error closing database resources: " + e.getMessage(), e);
            }
        }
    }
}