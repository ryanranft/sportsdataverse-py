# r_collector.R
#
# Purpose: Download NFL schedules, weekly rosters, and season rosters
#          via nflreadr, then save each as a Parquet file into
#          raw_data/nfl/<data_type>/<data_type>.parquet

# You may need to install these first:
# install.packages(c("nflreadr", "arrow", "fs"))

library(nflreadr)  # Provides load_schedules(), load_rosters_weekly(), load_rosters()
library(arrow)     # Provides write_parquet()
library(fs)        # Provides dir_create() for creating directories

collect_nfl_data <- function(base_dir = "/Users/ryanranft/0/sportsdataverse/sportsdataverse-py/raw_data") {
  # 1) NFL SCHEDULES for 2002–2025
  schedules <- load_schedules(2002:2025)

  # Create subdirectory: raw_data/nfl/nfl_schedule
  fs::dir_create(file.path(base_dir, "nfl", "nfl_schedule"))

  # Save as Parquet
  write_parquet(
    schedules,
    file.path(base_dir, "nfl", "nfl_schedule", "nfl_schedule.parquet")
  )
  message("Saved schedules to ", file.path(base_dir, "nfl", "nfl_schedule"))


  # 2) WEEKLY ROSTERS for 2002–2024
  weekly_rosters <- load_rosters_weekly(2002:2024)

  # Create subdirectory: raw_data/nfl/nfl_weekly_rosters
  fs::dir_create(file.path(base_dir, "nfl", "nfl_weekly_rosters"))

  # Save as Parquet
  write_parquet(
    weekly_rosters,
    file.path(base_dir, "nfl", "nfl_weekly_rosters", "nfl_weekly_rosters.parquet")
  )
  message("Saved weekly rosters to ", file.path(base_dir, "nfl", "nfl_weekly_rosters"))


  # 3) SEASON ROSTERS for 2002–2024
  rosters <- load_rosters(2002:2024)

  # Create subdirectory: raw_data/nfl/nfl_rosters
  fs::dir_create(file.path(base_dir, "nfl", "nfl_rosters"))

  # Save as Parquet
  write_parquet(
    rosters,
    file.path(base_dir, "nfl", "nfl_rosters", "nfl_rosters.parquet")
  )
  message("Saved season rosters to ", file.path(base_dir, "nfl", "nfl_rosters"))

  message("All requested datasets have been downloaded and saved into ", base_dir)
}

# If you run this file directly (e.g., Rscript r_collector.R), it will execute automatically:
if (sys.nframe() == 0) {
  collect_nfl_data(base_dir = "raw_data")
}