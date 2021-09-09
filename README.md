### Stream Processing of vital signal with Apache Flink
#### Stream processing with Apache Flink

This repo contains the code developed for the Master Thesis "Distributed Stream Processing and Modeling of Multivariate Temporal Sequences in the Medical Domain". It includes a Scala library for Flink, which contains different functions to perform preprocessing operations and feature construction over time series, on streaming.

Besides, it includes a pre-trained model for performing classification, as well as some datasets to test the functions implemented.

The code developed for Scala is available under src/main/scala directory. Folder util/ contains all the functions implemented: aggregation, feature extraction, interpolation, normalization, etc. Examples of their usage are available under experiments/. Moreover, a ProcessWindowFunction class, LSTMSequenceClassifier.scala was created in order to load a previously trained LSTM classifier and obtain classifications over data from a Flink window. Examples of the usage of this class are available under experiments/LSTMTest.scala and experiments/IntegratedExperiment.scala. This last experiment (experiments/IntegratedExperiment.scala) includes a complete workflow example in which raw time series from bedside monitors are fed to the program, they are processed, and then they are classified with the LSTM model as shock or non-shock sequences.

We also include code for extracting the data from MIMIC-III database (under data_preprocessing directory), as well as for training an LSTM network with the sequences generated with our functions (see src/main/scala/experiments/GenerateSignals.scala). The pre-trained model is available under src/main/resources/mimic2wdb/train-set/C1/lstm_model_vitals_new.

#### Generating raw signals time series + SOFA scores

We followed the methodology used by Hashmi, S. in her thesis in order to generate the SOFA scores for our signals. Thus, after installing the MIMIC-III clinical database and generating the tables and views for identifying sepsis patients ([available here](https://github.com/alistairewj/sepsis3-mimic)), we need to run the scripts available in the 'Generating time series of SOFA scores' folder in the following order:
1. urine_output.sql
2. vitals.sql
3. labs.sql
4. ventsettings.sql
5. ventdurations.sql
6. gcs.sql
7. echodata.sql
8. blood_gas.sql
9. bloodgasarterial.sql
10. Materialized views for SOFA query.sql
11. create table sepsis3_onsettime_new.sql
12. create table sepsis3_hourlysofa.sql
13. create table sepsis3_hourlysofa_entire_icustay.sql
14. Procedure sp_calc_hourly_SOFA.sql
15. create procedure sp_calc_hourly_SOFA_entire_icustay.sql 
16. updating sepsis3_cohort-new spesis_onsettime.sql

More information about this project is available in the thesis document.

Requirements: 
- Java 8.0 
- Scala 2.12
- Apache Flink 1.13.0

Links:
- [Apache Flink](https://flink.apache.org/)
- [Apache Application Development, DataStream API](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html)
- [A historical account of Apache Flink ](https://www.dima.tu-berlin.de/fileadmin/fg131/Informationsmaterial/Apache_Flink_Origins_for_Public_Release.pdf)



