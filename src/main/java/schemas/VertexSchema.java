package schemas;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class VertexSchema {

    public static StructType CreateVertex() {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("userid", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("utctimestamp", DataTypes.DateType, true));
        vertFields.add(DataTypes.createStructField("tpos", DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("venueid", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("venuecategory", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("cattype", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("vertextype", DataTypes.IntegerType, false));

        return DataTypes.createStructType(vertFields);
    }



}
