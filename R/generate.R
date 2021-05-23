################################################################################
# Joshua C. Fjelstul, Ph.D.
# r2sql R package
################################################################################

#' Generate an SQL script
#'
#' This function generates an SQL script that you can use to read datasets into
#' an SQL or MariaDB database. The use case for this function is that you have
#' one or more datasets, stored as \code{.csv} files in a local directory, or
#' existing as data frames in your current \code{R} workspace, that you want to
#' quickly read into an existing SQL or MariaDB database on a remote server.
#' This function will generate an SQL script that creates a table for each
#' dataset and quickly reads each dataset into the database.
#'
#' The SQL script will automatically overwrite without warning any table in the
#' database with the same name as one of the tables being created. You will get
#' a warning about this every time you run the function.
#'
#' You will need to store a \code{.csv} file for each dataset on your server in
#' the \code{server_path} directory before you run the SQL script or you will
#' get SQL errors about not being able to find the files. The function generates
#' these \code{.csv} files for you if you have not already prepared them and
#' will properly convert \code{NA} values in \code{R} to \code{\\N} values in
#' the SQL database.
#'
#' If any of your datasets include a variable whose name is a reserved word in
#' SQL, the function will automatically escape the variable name in the
#' generated script.
#'
#' @param input A list of data frames or a vector of \code{.csv} file names.
#' @param output_path The path for the local directory where the generated SQL
#'   script will be saved. The path should end with a \code{/} but the function
#'   will add one if it does not.
#' @param output_file The name of the generated SQL script without a file
#'   extension. The SQL script will be saved as a \code{.txt} file. If you do
#'   provide an extension, it will be replaced with \code{.txt}.
#' @param server_path The path for the folder on the server where you will store
#'   the \code{.csv} files to be added to the database. The path should end with
#'   a \code{/} but the function will add one if it does not. This path is
#'   required to generate the \code{INFLIE} command in the SQL script that will
#'   read the data into the database.
#' @param table_names A string vector that contains the names of the tables that
#'   should be created in the SQL database. There should be one element in the
#'   vector for each data frame or \code{.csv} file provided in the \code{input}
#'   argument.
#' @param generate_files Defaults to \code{TRUE}. If \code{TRUE}, the function
#'   will create a \code{.csv} file for each dataset in the \code{output_path}
#'   directory, which you can then transfer to your server. You need to put
#'   these files in the \code{server_path} directory that you specify in order
#'   for the script to work properly. The names of the files will be generated
#'   automatically and will correspond exactly to the names of the tables that
#'   will be created in the database.
#' @return Writes a \code{.txt} file with SQL commands and (optionally)
#'   \code{.csv} files in the \code{output_file} directory.
#' @export
generate <- function(input, table_names, output_path, output_file, server_path, generate_files = TRUE) {

  # create an empty list to hold the datasets
  datasets <- list()

  # check input
  if (class(input) == "list") {
    datasets <- input
  } else if (class(input) == "character") {
    if (all(stringr::str_detect(input, "\\.csv$"))) {
      for(i in 1:length(input)) {
        datasets[[i]] <- read.csv(input[i], stringsAsFactors = FALSE)
      }
    } else {
      stop("'input' should be a named list of data frames or a vector of .csv file names")
    }
  } else {
    stop("'input' should be a named list of data frames or a vector of .csv file names")
  }

  # check table names
  # if the user provides table names, overwrite the default names
  if (class(table_names) == "character") {
    if(length(table_names) == length(input)) {
      if(all(stringr::str_detect(table_names, "^[a-z][a-z0-9]+$"))) {
        names <- table_names
      } else {
        stop("'table_names' includes one or more invalid names")
      }
    } else {
      stop("'table_names' needs to be the same length as 'input'")
    }
  } else {
    stop("'table_names' needs to be a character vector")
  }

  # check output_path
  if( class(output_path) != "character") {
    stop("'output_path' needs to be a string")
  }
  if(length(output_path) != 1) {
    stop("'output_path' needs to be a string")
  }
  if (!stringr::str_detect(output_path, "/$")) {
    output_path <- stringr::str_c(output_path, "/")
  }

  # check server_path
  if( class(server_path) != "character") {
    stop("'server_path' needs to be a string")
  }
  if(length(server_path) != 1) {
    stop("'server_path' needs to be a string")
  }
  if (!stringr::str_detect(server_path, "/$")) {
    server_path <- stringr::str_c(server_path, "/")
  }

  # check output_file
  if( class(output_file) != "character") {
    stop("'output_file' needs to be a string")
  }
  if(length(output_file) != 1) {
    stop("'output_file' needs to be a string")
  }
  output_file <- stringr::str_remove(output_file, "\\.[A-Za-z]+$")
  output_file <- stringr::str_c(output_file, ".txt")

  # check generate_files
  if (class(generate_files) != "logical") {
    stop("'generate_files' needs to be TRUE or FALSE")
  }

  # create an empty list to store the script for each dataset
  script <- list()

  # generate scripts
  for (i in 1:length(datasets)) {

    # get the current dataset
    dataset_i <- datasets[[i]]

    # get the name of the dataset
    name_i <- table_names[1]

    # get the name of each variable
    variables_i <- names(dataset_i)
    variables_i[variables_i == "time"] <- "`time`"

    # get the class of each variable
    classes_i <- NULL
    for(j in 1:ncol(dataset_i)) {
      class <- class(dataset_i[, j])
      if(class == "character") {
        class <- "TEXT"
      } else if (class == "numeric" | class == "integer") {
        x <- dataset_i[, j]
        x <- x[!is.na(x)]
        if(all(x %% 1 == 0)) {
          class <- "INT"
        } else {
          class <- "FLOAT"
        }
      } else if (class == "Date") {
        class <- "DATE"
      } else {
        stop("column ", j, " in dataset ", i, " does not have a valid class")
      }
      classes_i <- c(classes_i, class)
    }

    # variable lines
    variables <- stringr::str_c("  ", variables_i, " ", classes_i, ",")
    variables[length(variables)] <- stringr::str_remove(variables[length(variables)], ",$")

    # generate the commands to create the table
    create_table <- c(
      stringr::str_c("DROP TABLE IF EXISTS ", name_i, ";"),
      stringr::str_c("CREATE TABLE ", name_i, "("),
      variables,
      ");"
    )

    # generate the commands to read in the table
    read_table <- c(
      stringr::str_c("LOAD DATA LOCAL INFILE '", server_path, name_i, ".csv'"),
      stringr::str_c("INTO TABLE ", name_i),
      "COLUMNS TERMINATED BY ','",
      "ENCLOSED BY '\"'",
      "LINES TERMINATED BY '\\n'",
      "IGNORE 1 LINES;"
    )

    # combine the commands
    commands <- c(
      create_table, "", read_table, ""
    )

    # add to list
    script[[i]] <- commands
  }

  # write the SQL script
  script <- unlist(script)
  script <- c("", "/* script generated automatically by the r2sql R package by Joshua C. Fjelstul, Ph.D. */", "", script)
  writeLines(script, stringr::str_c(output_path, output_file))

  # print note
  cat(stringr::str_c("OUTPUT: a .txt file with SQL commands was generated and saved to the directory \"", output_path, "\"\n"))

  # generate the CSV files
  if (generate_files == TRUE) {
    for (i in 1:length(datasets)) {
      write.csv(
        datasets[[i]],
        file = stringr::str_c(output_path, table_names[i], ".csv"),
        row.names = FALSE,
        quote = TRUE,
        na = "\\N"
      )
    }

    # print notes
    cat(stringr::str_c("OUTPUT: " , length(datasets), " .csv files were created and saved to the directory \"", output_path ,"\"\n"))
    cat(stringr::str_c("NOTE: you need to move the .csv files created by this function to your server and store them in the directory \"", output_path, "\" before you run the SQL commands or you will get SQL errors about not being able to find the files\n"))

  } else {

    # print note
    cat("NOTE: you need to add a .csv file for each dataset to your server and store them in the directory \"", server_path, "\" before you run the SQL commands or you will get SQL errors about not being able to find the files. The names of the files need to match the names in the generated SQL script. Use the option generate_files = TRUE to automatically generate these files\n")
  }

  # warning message
  message("WARNING: the generated SQL script will automatically overwrite without warning any existing tables in your SQL database with the same name as the tables you are creating")
}

################################################################################
# end R script
################################################################################
