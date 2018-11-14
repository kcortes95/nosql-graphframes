package schemas;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class EdgeSchema {

    public static StructType CreateEdge() {
        List<StructField> edgeFields = new ArrayList<StructField>();

        edgeFields.add(DataTypes.createStructField("src", DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("dst", DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("trajStep", DataTypes.BooleanType, true));
        edgeFields.add(DataTypes.createStructField("isVenue", DataTypes.BooleanType, true));
        edgeFields.add(DataTypes.createStructField("hasCategory", DataTypes.BooleanType, true));
        edgeFields.add(DataTypes.createStructField("subCategoryOf", DataTypes.BooleanType, true));
        edgeFields.add(DataTypes.createStructField("edgetype", DataTypes.IntegerType, true));

        return DataTypes.createStructType(edgeFields);
    }




}
