# Apache Spark Project
## Description
In this project, I use Apache Spark to collect data from distinct cnc tool manufacturers, clean them, trnsform them into consitent schema specified by the tool managment system, and perform some basic analysis.

## Dataset preview

Datasets are real life data provided by two, widly used, tool manufacturers. 
First subset, provided by Sandvik corporation contains:

|           File Name            |                                            Description                                             | File format |
|--------------------------------|----------------------------------------------------------------------------------------------------|-------------|
| package_assortment.xml         | Contains all items available in assortment. Each item has foreign keys that refers to other files. | xml         |
| gtc_class_hierarchy_vendor.xml | Contains names and descriptions in various languages                                               | xml         |
| product_data_files             | Set of product specific data files. Each file describe one product.                                | p21         |
| product_pictures               | Set of product images.                                                                             | jpg         |


Second subset, provided by Bitsbits contains:

|               File Name                |                Description                 | File format |
|----------------------------------------|--------------------------------------------|-------------|
| BitsBits Fusion Tool Library.V1.3.json | Contains all items available in assortment.| json        |
| bitsbitsImages                         | Set of product  images.                    | png         |

## Part 1 
- upload the files and join them by the foreign keys
- extract the required data from the struct
- create two dataframes, one for each producer
- transform dataframes into consistent schema
- unite both dataframes into one common tools set, with schema as shown below.
  
![](/src/main/schema.png)


## Part 2
Create basic functionality for data analysis and search, such as:
- group tools by description and show the body diameter range and average
- count tools with same diameter
- search tools by key words
- search by tool geometry (body diameter / body length).

## Part 3
- save the tool dataframe as parquet in hdfs
- save analysis results as csv files


 

