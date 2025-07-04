start.time <- Sys.time()

# utility functions ----------------------------------------------------------
get_last_month <- function() {
  last_day_of_last_month <- Sys.Date() - as.numeric(format(Sys.Date(), "%d")) - 1
  format(last_day_of_last_month, "%Y-%m")
}

calc_vi <- function(x) {
  as.integer(x * 0.01 / 1000) * 1000
}

# get command line arguments (flight month) -----------------------------------
args = commandArgs(trailingOnly = TRUE)
default_month <- get_last_month()

flight_month <- if (length(args) == 0) {
  get_last_month()
} else if (length(args) > 0 && args[1] == "--help") {
  last_month <- Sys.Date() - as.numeric(format(Sys.Date(), "%d")) - 1
  cat("Usage: Rscript ndc_api.R [YYYY-MM]\n")
  cat(paste("Default: last month --", default_month))
  quit(status = 0)
} else {
  args[1]
}
cat(paste("flight_month:", flight_month, "\n"))

# load required libraries -----------------------------------------------------
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tibble)
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
parquet_file_path <- glue("./data/ndc_api_{flight_month}.parquet")
csv_file_path <- glue("./data/ndc_api_{flight_month}.csv")

# check if output files already exist ----------------------------------------
if (!file.exists(parquet_file_path)) {
  # load JDBC driver and connect to Oracle database -----------------------------
  jdbcDriver <- JDBC(driverClass = "oracle.jdbc.OracleDriver", classPath = "D://instantclient_21.0/jdbc/lib/ojdbc11.jar")
  conn <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@lj.db.rep.mrva.io:1521/ORCL", "jinair_read", "hDtxZgzrfgXCtPv2QmEH")
  
  # query data from database ---------------------------------------------------
  sql <- glue("SELECT TO_CHAR(FTDA,'YYYY-MM-DD') FTDA, ORAC, DSTC, CARR, FTNR, TDNR, CPNR, 
  TYPE_DOCU TYPDOC, NPAX, TO_CHAR(DAIS,'YYYY-MM-DD') DAIS, AGTN, AGTN_NAME, CUTP, SRCI,
  RBKD, DOM_INT, PXTP, TRNC, CPVL, TMFA, YQ, YR_CUTP YR,
  TOTAL_VATC, ROUTING, BOON
FROM EDGAR_FIN.DWH_UPLIFT_STAT
WHERE SMOD = 'NEW' AND MNTH = '{flight_month}-01' AND 
  TACN = 718 AND TYPE_DOCU = 'T' AND
  ((AGTN = '17315174' AND BOON = 'SELOAI') OR 
   (AGTN = '17300091' AND BOON IN ('SELOAI', 'SELOI', 'SELTI')) OR
   (AGTN IN ('17315465','17334144','17334785') AND BOON IN ('SELOAI', 'SELTI')))
  ")
  
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
  col_select = c("FTDA", "AGTN", "AGTN_NAME", "TDNR", "CPNR", "CPVL", "BOON")
)

dt1 <- df |>
  group_by(AGTN, AGTN_NAME) |>
  summarise(
    CPVL = sum(CPVL, na.rm = TRUE),
  ) |>
  arrange(desc(CPVL)) |>
  mutate(
    VI = calc_vi(CPVL),
    VAT = calc_vi(CPVL) * 0.1,
    TTL = calc_vi(CPVL) + calc_vi(CPVL) * 0.1,
  )
print(dt1)

dt2 <- df |>
  group_by(AGTN, AGTN_NAME, BOON) |>
  summarise(
    CPVL = sum(CPVL, na.rm = TRUE),
    CNT = n(),
  ) |>
  # arrange(AGTN) |>
  mutate(
    VI = calc_vi(CPVL),
    # VAT = calc_vi(CPVL) * 0.1,
    # TTL = calc_vi(CPVL) + calc_vi(CPVL) * 0.1,
  )

print(dt2)

end.time <- Sys.time()
time.taken <- end.time - start.time
cat(paste("Time taken:", time.taken, "\n"))

# render rmarkdown report ---------------------------------------------------

# rmarkdown::render(
#   "ndc-api.Rmd",
#   output_file = paste0("ndc_api_", flight_month, ".html"),
#   params = list(flight_month = flight_month, parquet_file_path = parquet_file_path)
# )
