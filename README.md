# r2sql

This `R` package provides a function that generates an `SQL` script that you can use to read datasets into an `SQL` or `MariaDB` database. The use case for this package is that you have one or more datasets, stored as `.csv` files in a local directory, or existing as data frames in your current `R` workspace, that you want to quickly read into an existing `SQL` or `MariaDB` database on a remote server. This function will generate an `SQL` script that creates a table for each dataset and quickly reads each dataset into the database.

This package is currently under development. 
