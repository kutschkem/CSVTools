package kutschke.csv;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CSVSQL {

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            // load the driver into memory
            Class.forName("org.relique.jdbc.csv.CsvDriver");

            Properties prop = new Properties();
            prop.put("fileExtension", ".csv");
            prop.put("separator", ",");

            // create a connection. The first command line parameter is assumed
            // to
            // be the directory in which the .csv files are held
            Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + args[0], prop);

            // create a Statement object to execute the query with
            Statement stmt = conn.createStatement();

            BufferedReader rdr = new BufferedReader(new InputStreamReader(System.in));
            String resultString = "";
            while (true) {
                System.out.print("CSVSQL> ");
                String query = rdr.readLine();
                if ("exit".equals(query))
                    break;
                if (query.matches("toFile .*")) {
                    execToFile(query, resultString);
                }
                ResultSet results = stmt.executeQuery(query);

                resultString = printResultSet(results);
                System.out.println(resultString);

                // clean up
                results.close();
            }
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void execToFile(String query, String resultString) {
        // TODO Auto-generated method stub

    }

    private static String printResultSet(ResultSet results) throws SQLException {
        StringBuilder bldr = new StringBuilder();
        for (int i = 0; i < results.getMetaData().getColumnCount(); i++) {
            bldr.append(results.getMetaData().getColumnName(i + 1));
            bldr.append("\t| ");
        }
        bldr.append("\n");
        while (results.next()) {
            for (int i = 0; i < results.getMetaData().getColumnCount(); i++) {
                bldr.append(results.getString(i + 1));
                bldr.append("\t| ");
            }
            bldr.append('\n');
        }
        return bldr.toString();
    }

}
