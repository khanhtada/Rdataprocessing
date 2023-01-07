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
  library(knitr)
  #library(Microsoft365R)
  #library(blastula)
# CREATE INDICATORS METRICS----
info_server_func <- function() {
     table_list <- as.data.frame(dbListTables(con_bq_cr))
     colnames(table_list)[1]= "TABLE_NAME"
     table_list <- filter(table_list, substring(table_list[, 1], 1, 2) != "v_")
     row_num_arr = c()
    for (i in 1:nrow(table_list))
     {
      row_number <- as.numeric(dbGetQuery(con_bq_cr, paste0("select count(*)ROW_NUM from mart_risk.",  table_list[i, 1], " limit 10"))) # nolint
      row_num_arr <- c(row_num_arr, row_number)
     }
     table_list = mutate(table_list, row_num_arr) %>%  filter(row_num_arr !=0)
     message("List of tables in schema is:")
     return(table_list)
}
column_info <- function(table){
   df <- dbSendQuery(con_bq_cr, paste0("select * from mart_risk.",  table, " limit 10")) # nolint
        row_number <- as.numeric(dbGetQuery(con_bq_cr, paste0("select count(*)ROW_NUM from mart_risk.",  table, " limit 10"))) # nolint
        col_info <- mutate(as.data.frame(dbColumnInfo(df)),nrow = rep(row_number, nrow(as.data.frame(dbColumnInfo(df)))))
        return(col_info)
}
unique_value_func <- function(table){
 unique_value_arr = c()
        for (i in  1:nrow( col_info))
        {
            if (col_info$type[i] == "STRING" | col_info$type[i] == "INTEGER")
            {
            unique_value = as.numeric(dbGetQuery(con_bq_cr, paste0("select count(*) UNIQUE_VALUE from (select ",col_info$name[i], " from mart_risk.", table, " group by ",col_info$name[i],") t")))
            unique_value_arr = c(unique_value_arr, unique_value)
            } else 
        {
        unique_value_arr = c(unique_value_arr, NA)
        }
        
        } 
 
 
        return(unique_value_arr)
}
max_date_func <- function(table) {
max_date_arr = c()
        for (i in  1:nrow(col_info))
        {
          if(col_info$type[i] == "DATE" | col_info$type[i]  == "TIMESTAMP")
          {
            max_date_value = as.character(dbGetQuery(con_bq_cr, paste0("select cast(max(",col_info$name[i], ") as string)MAX_DATE from mart_risk.", table )))
            max_date_arr = c(max_date_arr, max_date_value)
          } else 
            max_date_arr = c(max_date_arr, NA)
        }
        return(max_date_arr)
}
min_date_func <- function(table){
        min_date_arr = c()
        for (i in  1:nrow(col_info))
        {
          if(col_info$type[i] == "DATE" | col_info$type[i]  == "TIMESTAMP")
          {
            min_date_value = as.character(dbGetQuery(con_bq_cr, paste0("select cast(min(",col_info$name[i], ") as string)MAX_DATE from mart_risk.", table)))
            min_date_arr = c(min_date_arr, min_date_value)
          } else 
            min_date_arr = c(min_date_arr, NA)
        }
        return(min_date_arr)
}
missing_value_count_func <- function(table){
        missing_value_count_arr = c()
        for (i in  1:nrow(col_info))
        {
          missing_value = as.numeric(dbGetQuery(con_bq_cr, paste0("select count(case when ",col_info$name[i], " is null then 1 end)NULL_VALUE from mart_risk.", table)))
          missing_value_count_arr = c(missing_value_count_arr, missing_value )
        }
        empty_value_count_arr = c()
          for (i in  1:nrow(col_info))
          {
             if (col_info$type[i] == "STRING"  )
         {
            empty_value = as.numeric(dbGetQuery(con_bq_cr, paste0("select count(case when ",col_info$name[i], " like '' then 1 end)EMPTY_VALUE from mart_risk.", table)))
            empty_value_count_arr = c(empty_value_count_arr, empty_value )
          }  else 
        {
          empty_value_count_arr = c(empty_value_count_arr, NA)
        }
          }
        return(data.frame(MISSING_VALUE = missing_value_count_arr,EMPTY_VALUE = empty_value_count_arr))
}

primary_key_find <- function(table){
  unique_value =  unique_value_func(table) 
  primary_key_arr = c()
  for (i in 1:nrow(col_info))
  {
    if (as.numeric(unique_value[i] / col_info$nrow[i]) == 1 && (col_info$type[i] == "STRING" | col_info$type[i] == "INTEGER"))
    {
      primary_key_arr = c(primary_key_arr, "primary_key")
    } else 
      primary_key_arr = c(primary_key_arr, "no")
  }
  return(primary_key_arr)
}
# HISTOGRAM---- 
# Distribution plot 
data_dist_plot <- function(table)
{  
   numeric_dim<-filter(col_info , type == "INTEGER")$name
  if (length(numeric_dim) == 0){
    message("There are no INTEGER dimension in table")
   } else if(max(col_info$nrow) == 0)
   {
    message("There are no rows in table")
   } else  
   { 
      par(mar=c(1,1,1,1)) #mfrow = c(ceiling(length(numeric_dim)/2),2)
      for (i in 1 : length(numeric_dim))
      { 
        data =   as.array(dbGetQuery(con_bq_cr,paste0("select ",numeric_dim[i], " from mart_risk.",table," where ",numeric_dim[i]," is not null"))[[1]])
        final_plot = hist(summary(data) , main = as.character(numeric_dim[i]) , xlab = "")
      }
      return(final_plot)
   } 
}
# Box plot 
data_box_plot <- function(table)
{  
   numeric_dim<-filter(col_info , type == "INTEGER")$name
  if (length(numeric_dim) == 0){
    message("There are no INTEGER dimension in table")
   } else if(max(col_info$nrow) == 0)
   {
    message("There are no rows in table")
   } else  
   {
      par(mar=c(1,1,1,1)) #mfrow = c(ceiling(length(numeric_dim)/2),2)
      for (i in 1 : length(numeric_dim))
      { 
        data =   as.array(dbGetQuery(con_bq_cr,paste0("select ",numeric_dim[i], " from mart_risk.",table," where ",numeric_dim[i]," is not null"))[[1]])
        final_plot = boxplot(summary(data) , main = as.character(numeric_dim[i]) , xlab = "")
      }
      return(final_plot)
   } 
}
#cat("Which schema you want to explore? (That function built for Bigquery DBMS)");
#    schema <- readLines("stdin",n=1);
library(knitr) 
library(pander)
library(flextable) 
library(dplyr)
library(officer)
# Get data set info 
con_bq_cr <- dbConnect(
   bigrquery::bigquery(),
    project = "bef-cake-prod",
    dataset = "mart_risk", #schema name to explore 
    billing = "bef-cake-prod")
    nrow(table_list)
# Function to generate the final output 
data_dic_gen <- function() {
    table_list = info_server_func()
    table_list_pr = autofit(flextable(table_list))
    table_list_pr = set_caption(table_list_pr, "Table list of Schema is: ")
    word_export <- read_docx()
    body_add_flextable(word_export, table_list_pr)
    for (i in 1:nrow(table_list)) #1:nrow(table_list)
    {
      message("Details of table ", table_list$TABLE_NAME[i], " is: ")
      col_info <- column_info(table_list$TABLE_NAME[i])
      final_df = rowwise(mutate(col_info,
      PRIMARY_KEY = primary_key_find(table_list$TABLE_NAME[i]),
      UNIQUE_VALUE = unique_value_func(table_list$TABLE_NAME[i])))
      final_df_2 = rowwise(mutate(name = final_df$name,
      MAX_DATA_DATE = max_date_func(table_list$TABLE_NAME[i]),
      MIN_DATA_DATE = min_date_func(table_list$TABLE_NAME[i]),
      missing_value_count_func(table_list$TABLE_NAME[i]))) 
      final_df = autofit(flextable(final_df))
      final_df = set_caption(final_df, caption = paste0(table_list$TABLE_NAME[i], " infos: "))
      final_df_2 = autofit(flextable(final_df_2))
      final_df_2 = set_caption(final_df_2, caption = paste0(table_list$TABLE_NAME[i], " infos: "))
      body_add_flextable(word_export, final_df)
      body_add_par(word_export, value = "")
      body_add_flextable(word_export, final_df_2)
      body_add_par(word_export, value = "")
      body_add_plot(word_export, data_dist_plot(table_list$TABLE_NAME[i]))
      body_add_plot(word_export, data_box_plot(table_list$TABLE_NAME[i])) 
     #dev.off() # Shut down current plotting device to fix “Error in plot.new() : figure margins too large” 
     # print(word_export, 'Cake_data_dictionary.docx') #Generate word file after gather all charts and table  
     }
     print(word_export, 'Cake_data_dictionary.docx')
}
data_dic_gen()


