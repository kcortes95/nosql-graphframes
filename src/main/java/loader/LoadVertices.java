package loader;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.stream.Stream;

public class LoadVertices {

    public static ArrayList<Row> LoadVertices(String categoryPath) {
        ArrayList<Row> vertList = new ArrayList<Row>();

        for (long rec = 0; rec <= 9; rec++) {
            vertList.add(RowFactory.create(rec, "0", new Timestamp(System.currentTimeMillis()), rec));
        }

        return vertList;
    }



    public static ArrayList<Row> LoadVerticesCategory2() {
        ArrayList<Row> vertList = new ArrayList<Row>();


        return vertList;
    }

    public static ArrayList<Row> LoadVerticesCategory(String path) {
        ArrayList<Row> vertList = new ArrayList<Row>();


        //path = "/Users/kevin/Desktop/categories.csv";

        try (Stream<String> stream = Files.lines(Paths.get(path))) {

            Object[] arr = stream.toArray();

            for(long i = 1 ; i < arr.length ; i++){
                vertList.add(RowFactory.create(i,arr[(int)i]));
                //System.out.println(arr[(int)i]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return vertList;
    }


}
