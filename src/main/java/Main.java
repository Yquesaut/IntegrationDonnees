import pl.coderion.model.ProductResponse;
import pl.coderion.service.OpenFoodFactsWrapper;
import pl.coderion.service.impl.OpenFoodFactsWrapperImpl;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
//        OpenFoodFactsWrapper wrapper = new OpenFoodFactsWrapperImpl();
//        ProductResponse productResponse = wrapper.fetchProductByCode("737628064502");
//
//        System.out.println(productResponse.toString());

        // create a SparkSession
        SparkSession sparkSession = SparkSession.builder().appName("Food Data").master("local").getOrCreate();
        //sparkSession.sparkContext().setLogLevel("WARN");

        // get the data
        String dataFile = "C:/Users/enzds/Cours/TP/I4/IntegDonnees/TP1/FoodData/openfoodfacts-products.jsonl";

        // Goal : we will try to predict if a passenger will survive or not

        // collect data into spark
        Dataset<Row> df = sparkSession.read()
                .format("json")
                .json(dataFile);

        // show the top 20 results
        df.show(20);

        sparkSession.stop();

//        System.out.println("rows number : " + df.count());
//        System.out.println("Columns number : " + Arrays.toString(df.columns()));
//        System.out.println("Data types : " + Arrays.toString(df.dtypes()));
//
//        // display
//        df.describe().show();
//
//        Dataset<Row> dataset = df.select(df.col("Survived").cast("float"),
//                df.col("Pclass").cast("float"),
//                df.col("Sex"),
//                df.col("Age").cast("float"),
//                df.col("Fare").cast("float"),
//                df.col("Embarked"));
//
//        /* Qualité des données - Filtrage, transformation de données */
//        // display all rows where age is null
//        dataset.show();
//
//        dataset.filter("Age is NULL").show();
//
//        // Replace the '?' values with null
//        for (String columnName : dataset.columns()) {
//            dataset = dataset.withColumn(columnName,
//                    functions.when(dataset.col(columnName).equalTo("?"), null).otherwise(dataset.col(columnName)));
//        }
//
//        dataset = dataset.na().drop();
//
//        // Index Sex Column
//        StringIndexerModel indexerSex = new StringIndexer()
//                .setInputCol("Sex")
//                .setOutputCol("Gender")
//                .setHandleInvalid("keep")
//                .fit(dataset);
//        dataset = indexerSex.transform(dataset);
//
//        // Index Embarked Column
//        StringIndexerModel indexerEmbarked = new StringIndexer()
//                .setInputCol("Embarked")
//                .setOutputCol("Boarded")
//                .setHandleInvalid("keep")
//                .fit(dataset);
//        dataset = indexerEmbarked.transform(dataset);
//
//        dataset.show();
//        // dataset.describe().show();
//
//        // check final data types
//        System.out.println(Arrays.toString(dataset.dtypes()));
//
//        // drop unnecessary columns - optimize model performance
//        dataset = dataset.drop("Sex");
//        dataset = dataset.drop("Embarked");
//        dataset.show();
//
//        // Create features column
//        // select necessary columns
//        String[] requiredFeatures = {"Pclass", "Age", "Fare", "Gender", "Boarded"};
//
//        Column[] selectedColumns = new Column[requiredFeatures.length];
//        for(int i=0; i < requiredFeatures.length; i++) {
//            selectedColumns[i] = dataset.col(requiredFeatures[i]);
//        }
//
//        // Use VectorAssembler to assemble the features
//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols(requiredFeatures)
//                .setOutputCol("features");
//
//        // Data transform
//        Dataset<Row> transformedData = assembler.transform(dataset);
//
//        // show transformed data
//        transformedData.show();
    }
}
