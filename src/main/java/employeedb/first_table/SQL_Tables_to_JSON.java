/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package employeedb.first_table;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author yassi
 */
public class SQL_Tables_to_JSON {

    public boolean transfer_JSON(String databaseName) throws IOException {
        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3308/" + databaseName, "root", "");
            Statement statement = connection.createStatement();
            String q = "SHOW tables";
            ResultSet tables = statement.executeQuery(q);
            File test = new File("C:\\Users\\yassi\\Documents\\NetBeansProjects\\com.project.data_conversion\\" + databaseName);
            boolean bool = test.mkdir();
            if (bool) {
                System.out.println("Directory created successfully");
            } else {
                System.out.println("Sorry couldn’t create specified directory");
            }
            while (tables.next()) {
                String table = tables.getString(1);
                System.out.print(table + "\n");
                Statement s = connection.createStatement();
                ResultSet resultSet = s.executeQuery("select * from " + table);

                // begin transfer
                int columnCount = -1, count = 0;
                JSONObject jSONObject = new JSONObject();
                JSONArray jSONArray = new JSONArray();
                while (resultSet.next()) {
                    if (columnCount == -1) {
                        columnCount = resultSet.getMetaData().getColumnCount();
                    } else {
                        JSONObject result = new JSONObject();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = resultSet.getMetaData().getColumnName(i);
                            Object value = resultSet.getObject(i);
                            result.put(columnName, value);
                            count++;
                        }
                        System.out.println(result);
                        jSONArray.add(result);
                        jSONObject.put(table, jSONArray);
                    }
                }

                try (FileWriter file = new FileWriter("C:\\Users\\yassi\\Documents\\NetBeansProjects\\com.project.data_conversion\\" + databaseName + "\\" + table + ".json")) {
                    file.write(jSONObject.toJSONString());
                }

                System.out.println("total transfer " + count + " lines!");

            }
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

// main for test
    public static void main(String[] args) throws IOException {
        new SQL_Tables_to_JSON().transfer_JSON("employe");
    }
}
