package loader;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class LoadEdges {

    public static ArrayList<Row> LoadEdges() {
        ArrayList<Row> edges = new ArrayList<Row>();

        /*
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        edges.add(RowFactory.create(0L, 1L, "trajStep"));
        edges.add(RowFactory.create(1L, 2L, "trajStep"));
        edges.add(RowFactory.create(2L, 3L, "trajStep"));
        edges.add(RowFactory.create(3L, 4L, "trajStep"));
        edges.add(RowFactory.create(4L, 5L, "trajStep"));
        edges.add(RowFactory.create(5L, 6L, "trajStep"));
        edges.add(RowFactory.create(6L, 7L, "trajStep"));
        edges.add(RowFactory.create(7L, 8L, "trajStep"));
        edges.add(RowFactory.create(8L, 9L, "trajStep"));
*/
        return edges;
    }
}
