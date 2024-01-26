package fr.integration.donnees.groupe.openfoodfacts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OpenFoodFactsApp {

    public static void main(String[] args) {
        // Créer une session Spark
        SparkSession spark = SparkSession.builder()
            .appName("OpenFoodFactsApp")
            .config("spark.master", "local")
            .getOrCreate();

        // Remplacez le chemin ci-dessous par le chemin complet de votre fichier CSV
        String csvFilePath = "C:/Users/Bassaoud/Downloads/en.openfoodfacts.org.products.csv";

        // Spécifier les paramètres de lecture du fichier CSV avec le bon délimiteur
        Map<String, String> options = new HashMap<>();
        options.put("header", "true"); // Spécifie que le fichier CSV a un en-tête
        options.put("inferSchema", "true"); // Spécifie que Spark doit inférer le schéma du fichier CSV
        options.put("delimiter", "\t"); // Spécifie le délimiteur comme une tabulation

        // Lire le fichier CSV en tant que DataFrame
        Dataset<Row> csvData = spark.read().options(options).csv(csvFilePath);

        // Nettoyage des données
        // Suppression des lignes avec des valeurs manquantes dans les colonnes nutritionnelles
        csvData = csvData
            .na().drop("any", new String[]{"energy_100g", "fat_100g", "carbohydrates_100g", "proteins_100g"})
            .filter("categories IS NOT NULL AND (energy_100g IS NOT NULL OR fat_100g IS NOT NULL OR carbohydrates_100g IS NOT NULL OR proteins_100g IS NOT NULL)");

        // Renommage des colonnes
        csvData = csvData
            .withColumnRenamed("product_name", "nom_produit")
            .withColumnRenamed("categories", "categories_produit");

        // Sélection des champs pertinents pour différents régimes
        Dataset<Row> selectedData = csvData.select(
            "nom_produit",
            "categories_produit",
            "origins",
            "brands",
            "energy_100g",
            "fat_100g",
            "carbohydrates_100g",
            "proteins_100g",
            "quantity",
            "packaging",
            "labels",
            "additives",
            "ingredients_text",
            "allergens",
            "labels_tags",
            "additives_tags",
            "nutriscore_score",
            "nutriscore_grade"
        );

        // Affichage des 20 premières lignes du DataFrame avec les champs sélectionnés
        selectedData.show();

        // Étape de stockage dans MySQL (remplacez les paramètres de connexion et le nom de la table selon vos besoins)
        String jdbcUrl = "jdbc:mysql://localhost:3306/base_de_donnees";
        String dbTable = "table";
        Properties dbProperties = new Properties();
        dbProperties.setProperty("user", "utilisateur");
        dbProperties.setProperty("password", "mot_de_passe");

        selectedData.write().jdbc(jdbcUrl, dbTable, dbProperties);
    }
}
