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
info_server_func <- function() {
     table_list <- as.data.frame(dbListTables(con_bq_cr))
     colnames(table_list)[1]= "TABLE_NAME"
     table_list <- filter(table_list, substring(table_list[, 1], 1, 2) != "v_")
     row_num_arr = c()
    for (i in 1:nrow(table_list))
     {
      row_number <- as.numeric(dbGetQuery(con_bq_cr, paste0("select count(*)ROW_NUM from risk_credit_data.",  table_list[i, 1], " limit 10"))) # nolint
      row_num_arr <- c(row_num_arr, row_number)
     }
     table_list = mutate(table_list, row_num_arr)
     message("List of tables in schema is:")
     return(table_list)
}
column_info <- function(table){
   df <- dbSendQuery(con_bq_cr, paste0("select * from risk_credit_data.",  table, " limit 10")) # nolint
        row_number <- as.numeric(dbGetQuery(con_bq_cr, paste0("select count(*)ROW_NUM from risk_credit_data.",  table, " limit 10"))) # nolint
        col_info <- mutate(as.data.frame(dbColumnInfo(df)),nrow = rep(row_number, nrow(as.data.frame(dbColumnInfo(df)))))
        return(col_info)
}
unique_value_func <- function(table){
 col_info <- column_info(table)
 unique_value_arr = c()
        for (i in  1:nrow(col_info))
        {
          
            unique_value = as.numeric(dbGetQuery(con_bq_cr, paste0("select count(*) UNIQUE_VALUE from (select ",col_info$name[i], " from risk_credit_data.", table, " group by ",col_info$name[i],") t")))
            unique_value_arr = c(unique_value_arr, unique_value)
        
        } 
        return(unique_value_arr)
}
max_date_func <- function(table) {
col_info <- column_info(table)
max_date_arr = c()
        for (i in  1:nrow(col_info))
        {
          if(col_info$type[i] == "DATE" | col_info$type[i]  == "TIMESTAMP")
          {
            max_date_value = as.character(dbGetQuery(con_bq_cr, paste0("select cast(max(",col_info$name[i], ") as string)MAX_DATE from risk_credit_data.", table )))
            max_date_arr = c(max_date_arr, max_date_value)
          } else 
            max_date_arr = c(max_date_arr, NA)
        }
        return(max_date_arr)
}
min_date_func <- function(table){
  col_info <- column_info(table)
        min_date_arr = c()
        for (i in  1:nrow(col_info))
        {
          if(col_info$type[i] == "DATE" | col_info$type[i]  == "TIMESTAMP")
          {
            min_date_value = as.character(dbGetQuery(con_bq_cr, paste0("select cast(min(",col_info$name[i], ") as string)MAX_DATE from risk_credit_data.", table)))
            min_date_arr = c(min_date_arr, min_date_value)
          } else 
            min_date_arr = c(min_date_arr, NA)
        }
        return(min_date_arr)
}
missing_value_count_func <- function(table){
  col_info <- column_info(table)
        missing_value_count_arr = c()
        for (i in  1:nrow(col_info))
        {
          missing_value = as.numeric(dbGetQuery(con_bq_cr, paste0("select count(case when ",col_info$name[i], " is null then 1 end)NULL_VALUE from risk_credit_data.", table)))
          missing_value_count_arr = c(missing_value_count_arr, missing_value )
        }
        empty_value_count_arr = c()
         for (i in  1:nrow(col_info))
        {
          empty_value = as.numeric(dbGetQuery(con_bq_cr, paste0("select count(case when cast(",col_info$name[i], " as string) like '' then 1 end)EMPTY_VALUE from risk_credit_data.", table)))
          empty_value_count_arr = c(empty_value_count_arr, empty_value )
        }
        return(data.frame(missing_value_count_arr, empty_value_count_arr))
}
primary_key_find <- function(table){
  col_info <- column_info(table)
  unique_value =  unique_value_func(table) 
  primary_key_arr = c()
  for (i in 1:nrow(col_info))
  {
    if (as.numeric(unique_value[i] / col_info$nrow[i]) == 1 )
    {
      primary_key_arr = c(primary_key_arr, "primary_key")
    } else 
      primary_key_arr = c(primary_key_arr, "no")
  }
  return(primary_key_arr)
}
# HISTOGRAM 
data_dist_plot <- function(table)
{
  numeric_dim<-filter(column_info(table), type == "INTEGER")$name
  if ( length(numeric_dim) == 0)
  {
   break
   } else 
      par(mfrow = c(floor(length(numeric_dim)/2),2))
      for (i in 1 : length(numeric_dim))
      { 
        data =   as.array(dbGetQuery(con_bq_cr,paste0("select ",numeric_dim[i], " from risk_credit_data.",table," where ",numeric_dim[i]," is not null"))[[1]])
        final_plot = hist(summary(data) , main = as.character(numeric_dim[i]) , xlab = "")
      }
      return(final_plot)
}

#-------------------

-------------FIXING
  numeric_dim<-filter(column_info("CL_CREDIT_CARD_DICTIONARY"), type == "INTEGER")$name
  if (length(numeric_dim) == 0){
   break
   } else  
      par(mfrow = c(floor(length(numeric_dim)/2),2))
      for (i in 1 : length(numeric_dim))
      { 
        data =   as.array(dbGetQuery(con_bq_cr,paste0("select ",numeric_dim[1], " from risk_credit_data.","CL_CREDIT_CARD_DICTIONARY"," where ",numeric_dim[i]," is not null"))[[1]])
        final_plot = hist(summary(data) , main = as.character(numeric_dim[i]) , xlab = "")
      }
      return(final_plot)
data_dist_plot("CL_CREDIT_CARD_DICTIONARY")
length(numeric_dim) == 0
#-----------------------------------
# HISTOGRAM 
data_box_plot <- function(table)
{
  numeric_dim<-filter(column_info(table), type == "INTEGER")$name
  par(mfrow = c(floor(length(numeric_dim)/2),2))
  for(i in 1 : length(numeric_dim))
  { 
    data =   as.array(dbGetQuery(con_bq_cr,paste0("select ",numeric_dim[i], " from risk_credit_data.",table," where ",numeric_dim[i]," is not null"))[[1]])
    final_plot = boxplot(data , main = as.character(numeric_dim[i]) , xlab = "")
  }
  return(final_plot)
}
#cat("Which schema you want to explore? (That function built for Bigquery DBMS)");
#    schema <- readLines("stdin",n=1);
con_bq_cr <- dbConnect(
   bigrquery::bigquery(),
    project = "bef-cake-prod",
    dataset = "risk_credit_data",
    billing = "bef-cake-prod")
data_dic_gen <- function()
{
    list_table = info_server_func()
    for (i in 2:nrow(list_table))
    {
      message("Details of table",list_table$TABLE_NAME[i] ,"is: ")
      final_df = mutate(column_info(list_table$TABLE_NAME[i]),
      unique_value_func(list_table$TABLE_NAME[i]), 
      max_date_func(list_table$TABLE_NAME[i]),
      min_date_func(list_table$TABLE_NAME[i]),
      missing_value_count_func(list_table$TABLE_NAME[i]),
      primary_key_find(list_table$TABLE_NAME[i]))
      print(final_df)
      data_dist_plot(list_table$TABLE_NAME[i])
      data_box_plot(list_table$TABLE_NAME[i])
  }
}
data_dic_gen()
par(mfrow = c(floor(length(numeric_dim)/2), 2)) 
