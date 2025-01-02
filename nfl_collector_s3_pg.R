# nfl_collector_s3_pg.R
#
# Purpose:
#   1. Download NFL schedules, weekly rosters, and season rosters via nflreadr.
#   2. Save each dataset locally as Parquet.
#   3. Upload each Parquet to S3.
#   4. Write each dataset into a Postgres table, but in the nfl schema.

# -- INSTALL PACKAGES IF NEEDED --
# install.packages(c("nflreadr", "arrow", "fs", "aws.s3", "DBI", "RPostgres"))

library(nflreadr)   # load_schedules(), load_rosters(), load_rosters_weekly()
library(arrow)      # write_parquet()
library(fs)         # dir_create() for creating directories
library(aws.s3)     # put_object(), etc.
library(DBI)        # dbConnect(), dbWriteTable(), dbExecute()
library(RPostgres)  # Postgres driver

collect_nfl_data_s3_pg <- function(
  base_dir     = "raw_data",
  s3_bucket    = Sys.getenv("S3_BUCKET", "sportsdataverse"),
  db_host      = Sys.getenv("POSTGRES_HOST", "localhost"),
  db_port      = Sys.getenv("POSTGRES_PORT", "5432"),
  db_name      = Sys.getenv("POSTGRES_DB", "sportsdataverse"),
  db_user      = Sys.getenv("POSTGRES_USER", "ryanranft"),
  db_pass      = Sys.getenv("POSTGRES_PASSWORD", "")
) {
  # ---------------------------
  # 1) CONNECT TO POSTGRES
  # ---------------------------
  # Adjust host, port, etc. as needed for your environment
  con <- dbConnect(
    RPostgres::Postgres(),
    host     = db_host,
    port     = db_port,
    dbname   = db_name,
    user     = db_user,
    password = db_pass
  )

  # Make sure the "nfl" schema exists
  dbExecute(con, "CREATE SCHEMA IF NOT EXISTS nfl")

  # HELPER FUNCTION:
  #   save data -> parquet -> s3 -> Postgres (nfl schema)
  save_and_upload <- function(df, data_type) {
    # 1) Create subdir (e.g., raw_data/nfl/nfl_schedule)
    subdir <- file.path(base_dir, "nfl", data_type)
    fs::dir_create(subdir)

    # 2) Write Parquet locally:
    local_path <- file.path(subdir, paste0(data_type, ".parquet"))
    arrow::write_parquet(df, local_path)
    message("Wrote ", data_type, " to ", local_path)

    # 3) Upload to S3 (e.g. s3://bucket/raw_data/nfl/nfl_schedule/nfl_schedule.parquet)
    if (nzchar(s3_bucket)) {
      s3_key <- file.path("raw_data", "nfl", data_type, paste0(data_type, ".parquet"))
      put_object(
        file   = local_path,
        object = s3_key,
        bucket = s3_bucket
      )
      message("Uploaded ", data_type, " to s3://", s3_bucket, "/", s3_key)
    } else {
      message("S3_BUCKET not set. Skipping S3 upload for ", data_type)
    }

    # 4) Write to Postgres in the nfl schema
    #    - We'll use the data_type as the table name.
    #    - If the table doesn't exist, create it. Otherwise, append.
    #    - By specifying DBI::Id(schema="nfl", table=data_type), the table goes into the nfl schema.
    dbWriteTable(
      conn      = con,
      name      = DBI::Id(schema = "nfl", table = data_type),
      value     = df,    # data frame (df is still a tibble/data.frame from nflreadr)
      append    = TRUE,  # or use "overwrite=TRUE" if you want to replace
      row.names = FALSE
    )
    message("Wrote data to Postgres table: nfl.", data_type)
  }

  # ---------------------------
  # 2) COLLECT THE DATA
  # ---------------------------
  # A) NFL SCHEDULES
  schedules <- load_schedules(2002:2025)
  save_and_upload(schedules, "nfl_schedule")

  # B) WEEKLY ROSTERS
  weekly_rosters <- load_rosters_weekly(2002:2024)
  save_and_upload(weekly_rosters, "nfl_weekly_rosters")

  # C) SEASON ROSTERS
  rosters <- load_rosters(2002:2024)
  save_and_upload(rosters, "nfl_rosters")

  message("All requested datasets have been downloaded, saved locally, uploaded to S3, and inserted into Postgres (nfl schema).")

  # CLOSE THE POSTGRES CONNECTION
  dbDisconnect(con)
}

# If you run this file directly (e.g., Rscript nfl_collector_s3_pg.R),
# it will execute automatically:
if (sys.nframe() == 0) {
  collect_nfl_data_s3_pg()
}