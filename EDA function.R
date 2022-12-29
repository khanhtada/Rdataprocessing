  library(groupdata2)
  library(psych)
  library(tidyr)
  library(bigrquery)
  library(DBI)
  library(dplyr)
  library(readxl)
  library(stringr)
  library(jsonlite)
  library(writexl)
  library(ggplot2)
  #library(Microsoft365R)
  #library(blastula)
  #I.Import data----
  #Create connection to BQ----
  con_bq_cr <- dbConnect(
    bigrquery::bigquery(),
    project = "bef-cake-prod",
    dataset = "risk_credit_data",
    billing = "bef-cake-prod")

eda_func <- function() {
    table_list <- filter(table_list, substring(table_list[, 1], 1, 2) != "v_")
    message("List of tables is chema is:")
    print(table_list)
    filter_table <- filter(table_list, table_list$dbListTables(con_bq_cr) )
    for (i in 1:length (table_list))
      message("Details of ", table_list[i], "is: ")
      df <- dbSendQuery(con_bq_cr, paste0("select * from risk_credit_data.",  table_list[i,1], " limit 10")) # nolint
      row_number <- dbSendQuery(con_bq_cr, paste0("select count(*)ROW_NUM from risk_credit_data.",  table_list[i], " limit 10")) # nolint
      column_info <- as.data.frame(dbColumnInfo(df))
      print(column_info)
    }
 df <- dbGetQuery(con_bq_cr, paste0("select count(*)ROW_NUM from risk_credit_data.",  table_list[1,1], " limit 10")) # nolint
