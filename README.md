# Apache Spark Project
## Description
In this project, I use Apache Spark to collect data from distinct cnc tools manufacturers, clean them, trnsform into consitent schema specified by tool managment system, and perform some basic analyze.

## Dataset preview
Datasets are real life data provided by two, widly used, tools manufacturers. 

First subset, provided by Sandvik corporation contains:
----------------------------------------------------------------------------------------------------------------------------------------------------
|           File Name            |                                            Description                                            | File format |
|--------------------------------|---------------------------------------------------------------------------------------------------|-------------|
| package_assortment.xml         | Contains all item available in assortment. Each item has foreign keys that refers to other files. | xml         |
| gtc_class_hierarchy_vendor.xml | Contains names and descriptions in various languages                                              | xml         |
| product_data_files             | Set of product specific data files. Each file describe one product.                               | p21         |
| product_pictures               | Set of products images.                                                                           | jpg         |
----------------------------------------------------------------------------------------------------------------------------------------------------

Second subset, provided by Bitsbits contains:
-----------------------------------------------------------------------------------------------------
|               File Name                |                Description                 | File format |
|----------------------------------------|--------------------------------------------|-------------|
| BitsBits Fusion Tool Library.V1.3.json | Contains all item available in assortment. | json        |
| bitsbitsImages                         | Set of products images.                    | png         |
-----------------------------------------------------------------------------------------------------

## Part 1 
- upload the files and join them by the foreign keys
- extract the required data from the struct
- create two dataframes, one for each producer
- transform dataframes into consisten schema
- union both dataframes into one common tools set, with schema as shown below.
  
![](/src/main/schema.png)


## Part 2
Create basic functionality for data analize and search, such as:
- group tools by description and show the body diameter range and average
- count tools with same diameters
- search tools by key words
- search by tool geometry (body diameter / body length).

## Part 3
- save the tool dataframe as parquet in hdfs
- save analysis results as csv files


 

