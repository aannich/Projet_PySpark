import os
from pyspark.sql.functions import avg, month, year, col, countDistinct,count
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt




def main():

    os.environ['PYSPARK_PYTHON'] = 'python'
    spark = SparkSession.builder \
        .appName("fil rouge") \
        .config("spark.master", "local") \
        .config("spark.driver.extraClassPath", "C:/Program Files/JDBC_PostgreSQL/postgresql-42.7.4.jar") \
        .config("spark.executor.extraClassPath", "C:/Program Files/JDBC_PostgreSQL/postgresql-42.7.4.jar") \
        .getOrCreate()

    jdbcUrl = "jdbc:postgresql://localhost:5432/filrouge"
    connectionProperties = {"user":"postgres","password":"annich"}
    listingsDF = spark.read.jdbc(jdbcUrl, "public.listings", properties=connectionProperties)
    reviewsDF = spark.read.jdbc(jdbcUrl, "public.reviews", properties=connectionProperties)

    listingsDF.createOrReplaceTempView("listings")
    reviewsDF.createOrReplaceTempView("reviews")

    listingsDF.printSchema()
    reviewsDF.printSchema()
    # 3. Filtrer les listings avec une disponibilité supérieure à 30 jours dans les 365.
    listingsDF.filter("availability_365 > 30 ").show()
    # 4. Calculer la moyenne des prix des listings pour chaque type de chambre
    listingsDF.groupby("room_type").agg(avg("price_clean").alias("AVG")).show()
    # 5. Extraire le mois à partir de la colonne "Date" dans les reviews.
    reviewsDF.select(month("date_review")).show()
    # 6. Ajouter une nouvelle colonne "Year" qui extrait l'année à partir de la colonne "Date_review"
    reviewsDF.withColumn("Year",year("date_review")).show()
    # 7. Filtrer les listings avec disponibilité supérieure à 30 jours et prix inférieur à 100 euros.
    listingsDF.filter("availability_365 > 30 AND price_clean < 100 ").show()
    # 8. Créer une nouvelle colonne "price_per_room" (prix par chambre) dans les listings
    listingsDF.withColumn("price_per_room",col("price_clean")/col("beds")).show()
    # 9. Remplacer les valeurs manquantes dans "reviews_per_month" par 0 dans les listings
    listingsDF.na.fill(0,subset="reviews_per_month").show()
    # 10.Remplacer les valeurs nulles dans la colonne "review_scores_rating" par la moyenne
    # calculée
    listingsDF.describe().show()
    listingsDF.na.fill(4.6893988540772575,subset="review_scores_rating").show()
    # Analyse des Données et Statistiques:
    # 11.Calculer la moyenne de la colonne "review_scores_rating".
    avg_review_scores_rating = listingsDF.agg(avg("review_scores_rating"))
    avg_review_scores_rating.show()
    # 12.Calculer la moyenne des prix pour chaque type de logement.
    listingsDF.groupby("property_type").agg(avg("price_clean").alias("AVGpartype")).show()
    # 13.Trouver le nombre total de types de chambre dans chaque zone voisine.
    listingsDF.groupby("neighbourhood").agg(countDistinct("room_type")).show()
    # 14.Calculer la corrélation entre le nombre de nuits disponibles et le prix des listings.
    correlation = listingsDF.stat.corr("availability_365","price_clean")
    print(correlation)
    # 15.Trouver les top 5 quartiers avec le plus grand nombre de listings
    listingsDF.groupby("neighbourhood_cleansed").agg(count("id").alias("nombre listing")).orderBy(col("nombre listing").desc()).show(5) 
    # PostgreSQL: Lire et Écrire:
    # 16.Compter le nombre total d'annonces dans chaque ville.
    listingsDF.createOrReplaceTempView("listings")
    nbrtotalannonce = spark.sql("SELECT neighbourhood, COUNT(*) AS nbrAnnonce FROM listings GROUP BY neighbourhood")
    nbrtotalannonce.show()
    # 18.Obtenir le nombre total d'avis dans la table des avis.
    reviewsDF.createOrReplaceTempView("reviews")

    nbrtotalavie = spark.sql("SELECT COUNT(*) AS TOTAL FROM reviews ")
    nbrtotalavie.show()
    # 19.Trouver la date de l'avis le plus récent dans la table des avis.
    date = spark.sql("SELECT MAX(date_review) FROM reviews")
    date.show()
    # 20.Identifier les cinq utilisateurs ayant publié le plus grand nombre d'avis.
    top5 = spark.sql("SELECT reviewer_name, count(*) AS nbrAvis  FROM reviews GROUP BY reviewer_name ORDER BY nbrAvis DESC LIMIT 5")
    top5.show()
    # 21.Compter le nombre d'hôtes uniques dans la table des listings.
    nbrHôte = spark.sql("SELECT count(Distinct host_id) FROM listings ")
    nbrHôte.show()
    # 22.Ajouter une colonne pour indiquer si le listing est premium et ajouter les résultats dans
    # une nouvelle table dans la base de données.
    listingsDF_premium = spark.sql("SELECT *, CASE WHEN price_clean > 150 THEN 'Premium' ELSE 'Standard' END AS type FROM listings ")
    listingsDF_premium.show()
    listingsDF_premium.write.mode("append").jdbc(jdbcUrl,"listings_premium",properties=connectionProperties)
# 23.Calculer le nombre moyen de nuits disponibles pour chaque hôte et ajouter les résultats
    # dans une nouvelle table dans la base de données.
    nbrNuitparHôte = spark.sql("SELECT host_id,AVG(availability_365) AS AVG_disponibilite FROM listings GROUP BY host_id")
    nbrNuitparHôte.show()
    nbrNuitparHôte.write.mode("append").jdbc(jdbcUrl,"nbrNuitparHôte",properties=connectionProperties)

    # 1. Bar Plot (Graphique en barres)
    moyennePrixParChambre = listingsDF.groupBy("room_type").agg(avg("price_clean").alias("prix_moyen"))
    #conversion du Dataframe pyspark en dataframe pandas
    pandas_moyennePrixParChambre = moyennePrixParChambre.toPandas()  # Convert to Pandas
    plt.figure(figsize=(10, 6))
    plt.title("Prix moyen par type de chambre", fontsize=16)
    plt.xlabel("Type de chambre")
    plt.ylabel("Prix moyen (en euros)")
    plt.bar(pandas_moyennePrixParChambre["room_type"],pandas_moyennePrixParChambre["prix_moyen"])
    plt.show()

    commentaireParMois = reviewsDF.groupBy(month("date_review").alias("Mois")).agg(count("id").alias("nbr_commentaire")).orderBy("nbr_commentaire")
    #conversion du Dataframe pyspark en dataframe pandas
    pandas_commentaireParMois = commentaireParMois.toPandas()  # Convert to Pandas
    plt.figure(figsize=(10, 6))
    plt.title("Nombre de commentaires par mois", fontsize=16)
    plt.xlabel("Nombre de commentaires)")
    plt.ylabel("mois")
    plt.plot(pandas_commentaireParMois["nbr_commentaire"],pandas_commentaireParMois["Mois"])
    plt.show()





if __name__ == "__main__":
    main()
