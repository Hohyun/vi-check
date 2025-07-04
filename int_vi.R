# load required libraries -----------------------------------------------------
library(tidyverse, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(rJava)
library(DBI)
library(RJDBC)
library(glue)
library(rmarkdown)

start.time <- Sys.time()
cat(paste("Start time:", start.time, "\n"))

# utility functions ----------------------------------------------------------
get_group <- function(agtn_name) {
  if (agtn_name == "INTER PARK TOUR CORPORATION" | agtn_name == "MYREALTRIP INC.") {
    return("A")
  } else {
    return("B")
  }
}

get_base_amount <- function(grp, amount) {
  t1 <- scheme |> filter(group == grp, sales_break < amount ) |> summarise(base_amount = max(sales_break))
  t1$base_amount
}

get_last_month <- function() {
  last_day_of_last_month <- Sys.Date() - as.numeric(format(Sys.Date(), "%d")) - 1
  format(last_day_of_last_month, "%Y-%m")
}

calc_vi <- function(agtn_name, x) {
  if (agtn_name == "INTER PARK TOUR CORPORATION") {
    return(500)
  }
  as.integer(x * 0.01 / 1000) * 1000
}

get_season <- function(month) {
  if (month %in% c(4, 6)) {
    return("L")
  } else if (month %in% c(5, 9, 10)) {
    return("N")
  } else if (month %in% c(7, 8)) {
    return("H")
  } else {
    return("")
  }
}

# get command line arguments (flight month) -----------------------------------
args = commandArgs(trailingOnly = TRUE)

default_month <- get_last_month()

flight_month <- if (length(args) == 0) {
  get_last_month()
} else if (length(args) > 0 && args[1] == "help") {
  last_month <- Sys.Date() - as.numeric(format(Sys.Date(), "%d")) - 1
  cat("Usage: Rscript int_vi.R [YYYY-MM]\n")
  cat(paste("Default: last month --", default_month))
  quit(status = 0)
} else {
  args[1]
}
cat(paste("flight_month:", flight_month, "\n"))

# set working directory -------------------------------------------------------
if (!dir.exists("D:/projects/vi-check")) {
  dir.create("D:/projects/vi-check", recursive = TRUE)
}
setwd("D:/projects/vi-check")

# define file paths for output files ------------------------------------------
parquet_file_path <- glue("./data/uplift_rpt_{flight_month}.parquet")
csv_file_path <- glue("./data/uplift_rpt_{flight_month}.csv")
szn_code <- get_season(as.integer(substr(flight_month, 6, 7)))

# load scheme data ------------------------------------------------------------
scheme <- read_csv_arrow(
  "./data/int_vi_scheme.csv"
)

scheme <- scheme |>
  filter(season == szn_code)

t1 <- scheme |> filter(group == 'B') |> summarise(min_sales = min(sales_break))
min_sales <- t1$min_sales

# load agent table -----------------------------------------------------------
agent_table <- read_csv_arrow(
  "./data/agent.csv",
  col_types = "cccc",
  col_names = c("AGTN", "MAIN_AGTN", "MAIN_AGTN_NAME", "BRANCH")
)

# check if output files already exist ----------------------------------------
if (!file.exists(parquet_file_path)) {
  # load JDBC driver and connect to Oracle database -----------------------------
  jdbcDriver <- JDBC(driverClass = "oracle.jdbc.OracleDriver", classPath = "D://instantclient_21.0/jdbc/lib/ojdbc11.jar")
  conn <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@lj.db.rep.mrva.io:1521/ORCL", "jinair_read", "hDtxZgzrfgXCtPv2QmEH")
  
  # query data from database ---------------------------------------------------
  sql <- glue("SELECT SRCI, AGTN, TO_CHAR(DAIS,'YYYY-MM-DD') DAIS, AGTN_NAME, PXNM, TRNC, CPNR, TDNR, 
	TACN, TO_CHAR(FTDA, 'YYYY-MM-DD) FTDA, FTNR, ORAC, DSTC, RBKD, TPAX, NPAX, EMIS_CUTP CUTP, 
	CPVL-CORT_COAM-CORT_SPAM-CPCM_OTHR NET_FARE, DOM_INT, ROUTING ROUTE
FROM EDGAR_FIN.DWH_UPLIFT_RPT dur 
WHERE SMOD = 'NEW' AND DOM_INT = 'INT' AND 
	TACN = '718' AND MNTH = '{flight_month}-01' AND
	SRCI IN ('AGTWKR', 'BSP-KR') AND  
	RBKD NOT IN ('G', 'G1', 'G2', 'G3', 'I')")
  
  df <- dbGetQuery(conn, sql)
  dbDisconnect(conn)
  
  # save results to parquet and CSV files --------------------------------------
  cat(paste("Writing data to", parquet_file_path, "and", csv_file_path, "\n"))
  write_parquet(df, parquet_file_path, compression = "snappy")
  write_csv_arrow(df, csv_file_path)
} else {
  cat(paste("Using existing file:", parquet_file_path, "\n"))
}


# calculate V/I for NDC API data ---------------------------------------------
df <- read_parquet(
  parquet_file_path, 
  col_select = c("FTDA", "AGTN", "AGTN_NAME", "TDNR", "CPNR", "CUTP", "NET_FARE", "NPAX", "DOM_INT", "ROUTE")
)

dt1 <- df |>
  filter(DOM_INT == "INT") |>
  group_by(AGTN, AGTN_NAME) |>
  summarise(
    NPAX = sum(NPAX, na.rm = TRUE),
    NET_FARE = sum(NET_FARE, na.rm = TRUE),
  ) 

dt2 <- dt1 |>
  left_join(agent_table, by = c("AGTN" = "AGTN")) |>
  mutate(
    MAIN_AGTN = if_else(is.na(MAIN_AGTN), AGTN, MAIN_AGTN),
    MAIN_AGTN_NAME = if_else(is.na(MAIN_AGTN_NAME), AGTN_NAME, MAIN_AGTN_NAME),
  ) |>
  group_by(MAIN_AGTN, MAIN_AGTN_NAME) |>
  summarise(
    OFC_CNT = n(),
    NPAX = sum(NPAX, na.rm = TRUE),
    NET_FARE = sum(NET_FARE, na.rm = TRUE),
  ) |>
  filter(
    NET_FARE >= min_sales
  ) |>
  arrange(desc(NET_FARE))
  

dt2 <- dt2 |>
  mutate(
    GRP = if_else(MAIN_AGTN %in% c("17315465", "17334155"), "A", "B")
  )

dt2 <- dt2 |>
  group_by(GRP, NET_FARE) |>
  mutate(
    BASE_AMOUNT = get_base_amount(GRP, NET_FARE),
  )
  
dt2 |>
  left_join(scheme, by = c("GRP" = "group", "BASE_AMOUNT" = "sales_break")) |>
  mutate(
    VI = BASE_AMOUNT * base_rate + (NET_FARE - BASE_AMOUNT) * over_rate,
  )

print(dt2)

end.time <- Sys.time()
cat(paste("\nEnd time:", end.time, "\n"))
time.taken <- end.time - start.time
cat(paste("Time taken:", time.taken, "\n"))

# render rmarkdown report ---------------------------------------------------

# rmarkdown::render(
#   "int_vi.Rmd",
#   output_file = paste0("int_vi_", flight_month, ".html"),
#   params = list(flight_month = flight_month, parquet_file_path = parquet_file_path)
# )
