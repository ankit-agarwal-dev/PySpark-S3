# PySpark Tutorial

Pyspark Tutorial is a script that explains in simple steps about few analytical operations after reading data from AWS S3 and writing data back into AWS S3 object store. This tutorial also briefly explains about using SQL in Pyspark.
Following concepts have been covered in this tutorial
* Creation of spark session
* Reading data form AWS S3 bucket
* Creating virtual tables in PySpark
* Joining virtual tables in PySpark for fetching values
* Retrieving various date related values from timestamp based EPOCH
* Writing data into AWS S3 bucket
     
## Installation

```python
pip install etl
```

## Usage

### Importing the libraries
```python
import etl
```

### Executing the scripts
```python
python etl.py
```
#### etl.py
**Pre Requisite**  
* AWS S3 bucket should be available with JSON data that will be utilised as input data. 

**Usage**  
This script will explain the various concepts for working on Pyspark. It briefly covers.
* Creation of Spark instance.
* Reading data from AWS S3 bucket.
* Performing operations on data retrieved.
* explaining concept pf working on Pyspark SQL.
* Writing data into AWS S3 bucket.

***Additional Script***

* dl.cfg  
Configuration files for AWS secret key. Please note that this has been intentionally kept blank. Please fill it with required values before starting the tutorial.

## Contributing
Any suggestions are welcome. For major changes, please open an issue first to discuss what you would like to change.   

## License
This is a public data and feel free to use it as necessary.