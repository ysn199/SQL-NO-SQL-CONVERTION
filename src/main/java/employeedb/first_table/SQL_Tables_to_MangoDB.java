/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package employeedb.first_table;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/**
 *
 * @author yassi
 */
public class SQL_Tables_to_MangoDB {

    public boolean transfer(String databaseName) throws IOException {
        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3308/"+databaseName, "root", "");
            Statement statement = connection.createStatement();
            String q = "SHOW tables";
            ResultSet tables = statement.executeQuery(q);

            //mongoDatabase.createCollection(tableName);
            //DBCollection collection=mongoDatabase.getCollection(tableName);
            while (tables.next()) {
                String table = tables.getString(1);
                System.out.print(table +"\n");
                Statement s = connection.createStatement();
                ResultSet resultSet = s.executeQuery("select * from "+table);
                // mongo connect
                MongoClient mongoClient = new MongoClient("localhost", 27017);
                MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
                MongoCollection<Document> collection = mongoDatabase.getCollection(table);
                // First empty the collection
                FindIterable<Document> deleteDocuments;
                deleteDocuments = collection.find();
                MongoCursor<Document> deleteIterator = deleteDocuments.iterator();
                while (deleteIterator.hasNext()) {
                    collection.deleteOne(deleteIterator.next());
                }
                // begin transfer
                int columnCount = -1, count = 0;
                List<Document> documents = new ArrayList<>();
                while (resultSet.next()) {
                    if (columnCount == -1) {
                        columnCount = resultSet.getMetaData().getColumnCount();
                    }
                    Document document = new Document();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = resultSet.getMetaData().getColumnName(i);
                        Object value = resultSet.getObject(i);
                        document.append(columnName, value);
                    }
                    documents.add(document);
                    count++;
                    System.out.println(count);
                    collection.insertMany(documents);
                    documents = new ArrayList<>();
                    System.out.println("transfer " + count + " lines from mysql to mongodb  ...");
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
        new SQL_Tables_to_MangoDB().transfer("classicmodels");
    }

}
