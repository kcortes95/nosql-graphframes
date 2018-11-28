package schemas;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class EdgeSchema {

    public static StructType CreateEdge() {
        List<StructField> edgeFields = new ArrayList<>();

        edgeFields.add(DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("neighbour", DataTypes.BooleanType, true));
        edgeFields.add(DataTypes.createStructField("hasCategory", DataTypes.BooleanType, true));
        edgeFields.add(DataTypes.createStructField("subCategoryOf", DataTypes.BooleanType, true));
        edgeFields.add(DataTypes.createStructField("edgetype", DataTypes.IntegerType, false));

        return DataTypes.createStructType(edgeFields);
    }




}
