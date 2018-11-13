package schemas;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Vertex {

    public static StructType CreateStopsVertex() {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("userid", DataTypes.StringType, false));
        vertFields.add(DataTypes.createStructField("utctimestamp", DataTypes.TimestampType , true));
        vertFields.add(DataTypes.createStructField("tpos", DataTypes.LongType, true));

        return DataTypes.createStructType(vertFields);
    }

    public static StructType CreateVenuesVertex() {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("venueid", DataTypes.StringType, false));

        return DataTypes.createStructType(vertFields);
    }

    public static StructType CreateCategoriesVertex() {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("venuecategory", DataTypes.StringType, false));

        return DataTypes.createStructType(vertFields);
    }

    public static StructType CreateCategoryVertex() {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("cattype", DataTypes.StringType, false));


        return DataTypes.createStructType(vertFields);
    }


}
