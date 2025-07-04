start.time <- Sys.time()

# utility functions ----------------------------------------------------------
get_last_month <- function() {
  last_day_of_last_month <- Sys.Date() - as.numeric(format(Sys.Date(), "%d")) - 1
  format(last_day_of_last_month, "%Y-%m")
}

calc_vi <- function(x) {
  as.integer(x * 0.01 / 1000) * 1000
}

clean_agtn_name <- function(agtn_name) {
  # remove everything inside parentheses and parentheses themselves
  agtn <- str_replace_all(agtn_name, "\\(.*?\\)", "")
  str_trim(agtn)
}

# get command line arguments (flight month) -----------------------------------
args = commandArgs(trailingOnly = TRUE)

default_month <- get_last_month()

flight_month <- if (length(args) == 0) {
  get_last_month()
} else if (length(args) > 0 && args[1] == "help") {
  last_month <- Sys.Date() - as.numeric(format(Sys.Date(), "%d")) - 1
  cat("Usage: Rscript dom_vi.R [YYYY-MM]\n")
  cat(paste("Default: last month --", default_month))
  quit(status = 0)
} else {
  args[1]
}
cat(paste("flight_month:", flight_month, "\n"))

# load required libraries -----------------------------------------------------
library(tidyverse, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(rJava)
library(DBI)
library(RJDBC)
library(glue)
library(rmarkdown)

# set working directory -------------------------------------------------------
if (!dir.exists("D:/projects/vi-check")) {
  dir.create("D:/projects/vi-check", recursive = TRUE)
}
setwd("D:/projects/vi-check")

# define file paths for output files ------------------------------------------
parquet_file_path <- glue("./data/flown_tkt_{flight_month}.parquet")
csv_file_path <- glue("./data/flown_tkt_{flight_month}.csv")

# check if output files already exist ----------------------------------------
if (!file.exists(parquet_file_path)) {
  # load JDBC driver and connect to Oracle database -----------------------------
  jdbcDriver <- JDBC(driverClass = "oracle.jdbc.OracleDriver", classPath = "D://instantclient_21.0/jdbc/lib/ojdbc11.jar")
  conn <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@lj.db.rep.mrva.io:1521/ORCL", "jinair_read", "hDtxZgzrfgXCtPv2QmEH")
  
  # query data from database ---------------------------------------------------
  sql <- glue("SELECT SRCI, AGTN, DAIS, AGTN_NAME, PXNM, TRNC, CPNR, TDNR, 
	TACN, FTDA, FTNR, ORAC, DSTC, RBKD, TPAX, NPAX, EMIS_CUTP CUTP, 
	CPVL-CORT_COAM-CORT_SPAM-CPCM_OTHR NET_FARE, DOM_INT, ROUTING ROUTE
FROM DWH_UPLIFT_RPT dur 
WHERE SMOD = 'NEW' AND DOM_INT = 'DOM' AND 
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
  col_select = c("FTDA", "AGTN", "AGTN_NAME", "TDNR", "CPNR", "CUTP", "NET_FARE", "DOM_INT", "ROUTE")
)

dt1 <- df |>
  filter(DOM_INT == "DOM") |>
  group_by(AGTN, AGTN_NAME) |>
  summarise(
    NET_FARE = sum(NET_FARE, na.rm = TRUE),
  ) 

dt2 <- dt1 |>
  mutate(AGTN_NAME = clean_agtn_name(AGTN_NAME)) |>
  group_by(AGTN_NAME) |>
  summarise(
    OFC_CNT = n(),
    NET_FARE = sum(NET_FARE, na.rm = TRUE),
  ) |>
  arrange(desc(NET_FARE)) |>
  mutate(
    VI = calc_vi(NET_FARE),
    VAT = calc_vi(NET_FARE) * 0.1,
    TTL = calc_vi(NET_FARE) + calc_vi(NET_FARE) * 0.1
  )

print(dt2)

end.time <- Sys.time()
time.taken <- end.time - start.time
cat(paste("Time taken:", time.taken, "\n"))
# render rmarkdown report ---------------------------------------------------

# rmarkdown::render(
#   "dom_vi.Rmd",
#   output_file = paste0("dom_vi_", flight_month, ".html"),
#   params = list(flight_month = flight_month, parquet_file_path = parquet_file_path)
# )
