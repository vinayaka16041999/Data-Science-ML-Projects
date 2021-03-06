// Databricks notebook source
val mushroom = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ",")
  .load("/FileStore/tables/mushrooms-3.csv")
mushroom.show()

// COMMAND ----------

mushroom.printSchema()

// COMMAND ----------

mushroom.describe().show()

// COMMAND ----------

mushroom.createOrReplaceTempView("MushroomData")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MushroomData

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(class), 
// MAGIC CASE 
// MAGIC 	WHEN class = "e" THEN "Edible"
// MAGIC 	ELSE "Poisonous"
// MAGIC END AS CLASSES,bruises from MushroomData group by CLASSES,bruises;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(capcolor), 
// MAGIC CASE 
// MAGIC 	WHEN capcolor = "n" THEN "Brown"
// MAGIC 	WHEN capcolor = "b" THEN "Buff"
// MAGIC 	WHEN capcolor = "c" THEN "Cinnamon"
// MAGIC 	WHEN capcolor = "g" THEN "Gray"
// MAGIC 	WHEN capcolor = "r" THEN "Green"
// MAGIC 	WHEN capcolor = "p" THEN "Pink"
// MAGIC 	WHEN capcolor = "u" THEN "Purple"
// MAGIC 	WHEN capcolor = "e" THEN "Red"
// MAGIC 	WHEN capcolor = "w" THEN "White"
// MAGIC 	ELSE "Yellow"
// MAGIC END AS ColorOfCap 
// MAGIC from MushroomData group by capcolor order by count(capcolor) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(capcolor),
// MAGIC Case
// MAGIC     when class="e" then "Edible"
// MAGIC     else "Poisonous"
// MAGIC end as Classes,    
// MAGIC CASE 
// MAGIC 	WHEN capcolor = "n" THEN "Brown"
// MAGIC 	WHEN capcolor = "b" THEN "Buff"
// MAGIC 	WHEN capcolor = "c" THEN "Cinnamon"
// MAGIC 	WHEN capcolor = "g" THEN "Gray"
// MAGIC 	WHEN capcolor = "r" THEN "Green"
// MAGIC 	WHEN capcolor = "p" THEN "Pink"
// MAGIC 	WHEN capcolor = "u" THEN "Purple"
// MAGIC 	WHEN capcolor = "e" THEN "Red"
// MAGIC 	WHEN capcolor = "w" THEN "White"
// MAGIC 	ELSE "Yellow"
// MAGIC END AS ColorOfCap 
// MAGIC from MushroomData group by capcolor,class order by count(capcolor) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(odor),
// MAGIC case
// MAGIC     WHEN odor = "a" THEN "almond"
// MAGIC 	WHEN odor = "l" THEN "anise"
// MAGIC 	WHEN odor = "c" THEN "creosote"
// MAGIC 	WHEN odor = "y" THEN "fishy"
// MAGIC 	WHEN odor = "f" THEN "foul"
// MAGIC 	WHEN odor = "m" THEN "musty"
// MAGIC 	WHEN odor = "n" THEN "none"
// MAGIC 	WHEN odor = "p" THEN "pungent"
// MAGIC 	ELSE "spicy"
// MAGIC end as Odor
// MAGIC from MushroomData group by odor order by count(odor) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(odor),
// MAGIC case
// MAGIC when class = "e" then "Edible"
// MAGIC else "Poisonous"
// MAGIC end as Classes,
// MAGIC case 
// MAGIC   WHEN odor = "a" THEN "almond"
// MAGIC 	WHEN odor = "l" THEN "anise"
// MAGIC 	WHEN odor = "c" THEN "creosote"
// MAGIC 	WHEN odor = "y" THEN "fishy"
// MAGIC 	WHEN odor = "f" THEN "foul"
// MAGIC 	WHEN odor = "m" THEN "musty"
// MAGIC 	WHEN odor = "n" THEN "none"
// MAGIC 	WHEN odor = "p" THEN "pungent"
// MAGIC 	ELSE "spicy"
// MAGIC end as Odor
// MAGIC from MushroomData group by odor,class order by count(odor) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(population),
// MAGIC case
// MAGIC WHEN population = "a" THEN "abundant"
// MAGIC 	WHEN population = "c" THEN "clustered"
// MAGIC 	WHEN population = "n" THEN "numerous"
// MAGIC 	WHEN population = "s" THEN "scattered"
// MAGIC 	WHEN population = "v" THEN "several"
// MAGIC 	ELSE "solitary"
// MAGIC     end as Population
// MAGIC     from MushroomData group by population order by count(population) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(population),
// MAGIC case
// MAGIC when class="e" then "Edible"
// MAGIC else "Poisonous"
// MAGIC end as Classes,
// MAGIC case
// MAGIC WHEN population = "a" THEN "abundant"
// MAGIC 	WHEN population = "c" THEN "clustered"
// MAGIC 	WHEN population = "n" THEN "numerous"
// MAGIC 	WHEN population = "s" THEN "scattered"
// MAGIC 	WHEN population = "v" THEN "several"
// MAGIC 	ELSE "solitary"
// MAGIC     end as Population
// MAGIC     from MushroomData group by population,class order by count(population) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(habitat),
// MAGIC case
// MAGIC WHEN habitat = "g" THEN "grasses"
// MAGIC 	WHEN habitat = "l" THEN "leaves"
// MAGIC 	WHEN habitat = "m" THEN "meadows"
// MAGIC 	WHEN habitat = "p" THEN "paths"
// MAGIC 	WHEN habitat = "u" THEN "urban"
// MAGIC 	WHEN habitat = "w" THEN "waste"
// MAGIC 	ELSE "wood"
// MAGIC     end as Habitat
// MAGIC     from MushroomData group by habitat order by count(habitat) desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(habitat),
// MAGIC case
// MAGIC when class="e" then "Edible"
// MAGIC else "Poisonous"
// MAGIC end as Classes,
// MAGIC case
// MAGIC WHEN habitat = "g" THEN "grasses"
// MAGIC 	WHEN habitat = "l" THEN "leaves"
// MAGIC 	WHEN habitat = "m" THEN "meadows"
// MAGIC 	WHEN habitat = "p" THEN "paths"
// MAGIC 	WHEN habitat = "u" THEN "urban"
// MAGIC 	WHEN habitat = "w" THEN "waste"
// MAGIC 	ELSE "wood"
// MAGIC     end as Habitat
// MAGIC     from MushroomData group by habitat,class order by count(habitat) desc;

// COMMAND ----------

var StringfeatureCol = Array("class", "capshape", "capsurface", "capcolor", "bruises", "odor", "gillattachment", "gillspacing", "gillsize", "gillcolor", "stalkshape", "stalkroot", "stalksurfaceabovering", "stalksurfacebelowring", "stalkcolorabovering", "stalkcolorbelowring", "veiltype", "veilcolor", "ringnumber", "ring-type", "sporeprintcolor", "population", "habitat")

// COMMAND ----------

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}

val indexers = StringfeatureCol.map { colName =>
  new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed")
}

val pipeline = new Pipeline()
                    .setStages(indexers)      

val mushroomDF = pipeline.fit(mushroom).transform(mushroom)

// COMMAND ----------

display(mushroomDF)

// COMMAND ----------

mushroomDF.printSchema()

// COMMAND ----------

val splits = mushroomDF.randomSplit(Array(0.7, 0.3))
val train = splits(0)
val test = splits(1)
val train_rows = train.count()
val test_rows = test.count()
println("Training Rows: " + train_rows + " Testing Rows: " + test_rows)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler().setInputCols(Array("capshape_indexed", "capsurface_indexed", "capcolor_indexed", "bruises_indexed", "odor_indexed",
"gillattachment_indexed", "gillspacing_indexed", "gillsize_indexed", "gillcolor_indexed", "stalkshape_indexed", "stalkroot_indexed",
"stalksurfaceabovering_indexed", "stalksurfacebelowring_indexed", "stalkcolorabovering_indexed", "stalkcolorbelowring_indexed", 
"veiltype_indexed", "veilcolor_indexed", "ringnumber_indexed", "ring-type_indexed", "sporeprintcolor_indexed", "population_indexed", "habitat_indexed")).setOutputCol("features")

val training = assembler.transform(train).select($"features", $"class_indexed".alias("label"))

training.show(false)

// COMMAND ----------

import org.apache.spark.ml.classification.LogisticRegression

val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(10).setRegParam(0.3)
val model = lr.fit(training)
println("Model Trained!")

// COMMAND ----------

val testing = assembler.transform(test).select($"features", $"class_indexed".alias("trueLabel"))
testing.show()

// COMMAND ----------

val prediction = model.transform(testing)
val predicted = prediction.select("features", "prediction", "trueLabel")
display(predicted)

// COMMAND ----------

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

val evaluator = new BinaryClassificationEvaluator().setLabelCol("trueLabel").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
val auc = evaluator.evaluate(prediction)
println("AUC = " + (auc))

// COMMAND ----------

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")

val model = dt.fit(training)

println("Model Trained!")

// COMMAND ----------

val prediction = model.transform(testing)
val predicted = prediction.select("features", "prediction", "trueLabel")
predicted.show()

// COMMAND ----------

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("trueLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")
val accuracy = evaluator.evaluate(prediction)

// COMMAND ----------


