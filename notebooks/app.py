# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ArrayType,
    IntegerType
)

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # json message schema
    json_schema = StructType(
        [
            StructField("event_date", StringType()
                       ),
            StructField("data", ArrayType(StructType([StructField("iso_code", StringType()),
                                                      StructField("continent", StringType()),
                                                      StructField("location", StringType()),
                                                      StructField("total_cases", FloatType()),
                                                      StructField("new_cases", FloatType()),
                                                      StructField("new_cases_smoothed", FloatType()),
                                                      StructField("total_deaths", FloatType()),
                                                      StructField("new_deaths", FloatType()),
                                                      StructField("new_deaths_smoothed", FloatType()),
                                                      StructField("total_cases_per_million", FloatType()),
                                                      StructField("new_cases_per_million", FloatType()),
                                                      StructField("new_cases_smoothed_per_million", FloatType()),
                                                      StructField("total_deaths_per_million", FloatType()),
                                                      StructField("new_deaths_per_million", FloatType()),
                                                      StructField("new_deaths_smoothed_per_million", FloatType()),
                                                      StructField("reproduction_rate", FloatType()),
                                                      StructField("icu_patients", FloatType()),
                                                      StructField("icu_patients_per_million", FloatType()),
                                                      StructField("hosp_patients", FloatType()),
                                                      StructField("hosp_patients_per_million", FloatType()),
                                                      StructField("weekly_icu_admissions", FloatType()),
                                                      StructField("weekly_icu_admissions_per_million", FloatType()),
                                                      StructField("weekly_hosp_admissions", FloatType()),
                                                      StructField("weekly_hosp_admissions_per_million", FloatType()),
                                                      StructField("total_tests", FloatType()),
                                                      StructField("new_tests", FloatType()),
                                                      StructField("total_tests_per_thousand", FloatType()),
                                                      StructField("new_tests_per_thousand", FloatType()),
                                                      StructField("new_tests_smoothed", FloatType()),
                                                      StructField("new_tests_smoothed_per_thousand", FloatType()),
                                                      StructField("positive_rate", FloatType()),
                                                      StructField("tests_per_case", FloatType()),
                                                      StructField("tests_units", StringType()),
                                                      StructField("total_vaccinations", FloatType()),
                                                      StructField("people_vaccinated", FloatType()),
                                                      StructField("people_fully_vaccinated", FloatType()),
                                                      StructField("total_boosters", FloatType()),
                                                      StructField("new_vaccinations", FloatType()),
                                                      StructField("new_vaccinations_smoothed", FloatType()),
                                                      StructField("total_vaccinations_per_hundred", FloatType()),
                                                      StructField("people_vaccinated_per_hundred", FloatType()),
                                                      StructField("people_fully_vaccinated_per_hundred", FloatType()),
                                                      StructField("total_boosters_per_hundred", FloatType()),
                                                      StructField("new_vaccinations_smoothed_per_million", FloatType()),
                                                      StructField("new_people_vaccinated_smoothed", FloatType()),
                                                      StructField("new_people_vaccinated_smoothed_per_hundred", FloatType()),
                                                      StructField("stringency_index", FloatType()),
                                                      StructField("population_density", FloatType()),
                                                      StructField("median_age", FloatType()),
                                                      StructField("aged_65_older", FloatType()),
                                                      StructField("aged_70_older", FloatType()),
                                                      StructField("gdp_per_capita", FloatType()),
                                                      StructField("extreme_poverty", FloatType()),
                                                      StructField("cardiovasc_death_rate", FloatType()),
                                                      StructField("diabetes_prevalence", FloatType()),
                                                      StructField("female_smokers", FloatType()),
                                                      StructField("male_smokers", FloatType()),
                                                      StructField("handwashing_facilities", FloatType()),
                                                      StructField("hospital_beds_per_thousand", StringType()),
                                                      StructField("life_expectancy", FloatType()),
                                                      StructField("human_development_index", FloatType()),
                                                      StructField("population", FloatType()),
                                                      StructField("excess_mortality_cumulative_absolute", FloatType()),
                                                      StructField("excess_mortality_cumulative", FloatType()),
                                                      StructField("excess_mortality", FloatType()),
                                                      StructField("excess_mortality_cumulative_per_million", FloatType()),
                                                      
                                                      
                                                     ]
                                                    )
                                         )
                       )

        ]
    )
    
    # topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "test")
        .load()
    )


    parsed = raw.select(
       f.from_json(raw.value.cast("string"), json_schema).alias("json")).select(
        f.col("json").getField("event_date").alias("event_date"),
        f.explode(f.col("json").getField("data")).alias("data")).select(
        f.col("event_date"),
        f.col("data.iso_code").alias("iso_code"),
        f.col("data.continent").alias("continent"),
        f.col("data.location").alias("location"),
        f.col("data.total_cases").alias("total_cases"),
        f.col("data.new_cases").alias("new_cases"),
        f.col("data.new_cases_smoothed").alias("new_cases_smoothed"),
        f.col("data.total_deaths").alias("total_deaths"),
        f.col("data.new_deaths").alias("new_deaths"),
        f.col("data.new_deaths_smoothed").alias("new_deaths_smoothed"),
        f.col("data.total_cases_per_million").alias("total_cases_per_million"),
        f.col("data.new_cases_per_million").alias("new_cases_per_million"),
        f.col("data.new_cases_smoothed_per_million").alias("new_cases_smoothed_per_million"),
        f.col("data.total_deaths_per_million").alias("total_deaths_per_million"),
        f.col("data.new_deaths_per_million").alias("new_deaths_per_million"),
        f.col("data.new_deaths_smoothed_per_million").alias("new_deaths_smoothed_per_million"),
        f.col("data.reproduction_rate").alias("reproduction_rate"),
        f.col("data.icu_patients").alias("icu_patients"),
        f.col("data.icu_patients_per_million").alias("icu_patients_per_million"),
        f.col("data.hosp_patients").alias("hosp_patients"),
        f.col("data.hosp_patients_per_million").alias("hosp_patients_per_million"),
        f.col("data.weekly_icu_admissions").alias("weekly_icu_admissions"),
        f.col("data.weekly_icu_admissions_per_million").alias("weekly_icu_admissions_per_million"),
        f.col("data.weekly_hosp_admissions").alias("weekly_hosp_admissions"),
        f.col("data.weekly_hosp_admissions_per_million").alias("weekly_hosp_admissions_per_million"),
        f.col("data.total_tests").alias("total_tests"),
        f.col("data.new_tests").alias("new_tests"),
        f.col("data.total_tests_per_thousand").alias("total_tests_per_thousand"),
        f.col("data.new_tests_per_thousand").alias("new_tests_per_thousand"),
        f.col("data.new_tests_smoothed").alias("new_tests_smoothed"),
        f.col("data.new_tests_smoothed_per_thousand").alias("new_tests_smoothed_per_thousand"),
        f.col("data.positive_rate").alias("positive_rate"),
        f.col("data.tests_per_case").alias("tests_per_case"),
        f.col("data.tests_units").alias("tests_units"),
        f.col("data.total_vaccinations").alias("total_vaccinations"),
        f.col("data.people_vaccinated").alias("people_vaccinated"),
        f.col("data.people_fully_vaccinated").alias("people_fully_vaccinated"),
        f.col("data.total_boosters").alias("total_boosters"),
        f.col("data.new_vaccinations").alias("new_vaccinations"),
        f.col("data.new_vaccinations_smoothed").alias("new_vaccinations_smoothed"),
        f.col("data.total_vaccinations_per_hundred").alias("total_vaccinations_per_hundred"),
        f.col("data.people_vaccinated_per_hundred").alias("people_vaccinated_per_hundred"),
        f.col("data.people_fully_vaccinated_per_hundred").alias("people_fully_vaccinated_per_hundred"),
        f.col("data.total_boosters_per_hundred").alias("total_boosters_per_hundred"),
        f.col("data.new_vaccinations_smoothed_per_million").alias("new_vaccinations_smoothed_per_million"),
        f.col("data.new_people_vaccinated_smoothed").alias("new_people_vaccinated_smoothed"),
        f.col("data.new_people_vaccinated_smoothed_per_hundred").alias("new_people_vaccinated_smoothed_per_hundred"),
        f.col("data.stringency_index").alias("stringency_index"),
        f.col("data.population_density").alias("population_density"),
        f.col("data.median_age").alias("median_age"),
        f.col("data.aged_65_older").alias("aged_65_older"),
        f.col("data.aged_70_older").alias("aged_70_older"),
        f.col("data.gdp_per_capita").alias("gdp_per_capita"),
        f.col("data.extreme_poverty").alias("extreme_poverty"),
        f.col("data.cardiovasc_death_rate").alias("cardiovasc_death_rate"),
        f.col("data.diabetes_prevalence").alias("diabetes_prevalence"),
        f.col("data.female_smokers").alias("female_smokers"),
        f.col("data.male_smokers").alias("male_smokers"),
        f.col("data.handwashing_facilities").alias("handwashing_facilities"),
        f.col("data.hospital_beds_per_thousand").alias("hospital_beds_per_thousand"),
        f.col("data.life_expectancy").alias("life_expectancy"),
        f.col("data.human_development_index").alias("human_development_index"),
        f.col("data.population").alias("population"),
        f.col("data.excess_mortality_cumulative_absolute").alias("excess_mortality_cumulative_absolute"),
        f.col("data.excess_mortality_cumulative").alias("excess_mortality_cumulative"),
        f.col("data.excess_mortality").alias("excess_mortality"),
        f.col("data.excess_mortality_cumulative_per_million").alias("excess_mortality_cumulative_per_million"),

        
    )



    # defining output
    query = parsed.writeStream.outputMode("append").format("console").start()
    # query.awaitTermination(60)
    query.stop()
    

    


