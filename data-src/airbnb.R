library(tidyverse)
library(xml2)
library(httr2)
library(future)
library(future.apply)
library(here)
tmp_dir <- here("data")

doc <- read_html("http://insideairbnb.com/get-the-data")
all_links <- xml_find_all(doc, '//*[@id="gatsby-focus-wrapper"]/div/div[2]/table//a') |>
  xml_attr("href")
data_infos <- data.frame(
  url = grep("/data/", all_links, fixed = TRUE, value = TRUE)
) |>
  mutate(
    path = sapply(url, function(z) url_parse(z)$path)
  ) |>
  filter(
    str_count(path, "/") == 6
  ) |>
  separate(path,
    into = c("dummy", "country", "state", "city", "date", "data", "basename"),
    sep = "/"
  ) |>
  select(-dummy) |>
  filter(city %in% c("amsterdam", "athens", "barcelona", "berlin", "bordeaux", "brussels", "dublin", "florence"))

as_filename <- function(url, country, state, city, date) {
  filename <- paste(date, country, state, city, sep = "-")
  filename <- paste0(filename, "-", gsub(".csv.gz", "", basename(url), fixed = TRUE), ".parquet")
  filename <- file.path(tmp_dir, filename)
  filename
}

read_listing <- function(url) {
  read_csv(
    file = url,
    col_types = cols(
      id = col_character(),
      listing_url = col_skip(),
      scrape_id = col_skip(),
      last_scraped = col_skip(),
      source = col_skip(),
      name = col_character(),
      description = col_character(),
      neighborhood_overview = col_skip(),
      picture_url = col_skip(),
      host_id = col_double(),
      host_url = col_skip(),
      host_name = col_character(),
      host_since = col_skip(),
      host_location = col_character(),
      host_about = col_skip(),
      host_response_time = col_character(),
      host_response_rate = col_skip(),
      host_acceptance_rate = col_skip(),
      host_is_superhost = col_logical(),
      host_thumbnail_url = col_skip(),
      host_picture_url = col_skip(),
      host_neighbourhood = col_skip(),
      host_listings_count = col_skip(),
      host_total_listings_count = col_skip(),
      host_verifications = col_skip(),
      host_has_profile_pic = col_skip(),
      host_identity_verified = col_skip(),
      neighbourhood = col_skip(),
      neighbourhood_cleansed = col_skip(),
      neighbourhood_group_cleansed = col_skip(),
      latitude = col_skip(),
      longitude = col_skip(),
      property_type = col_character(),
      room_type = col_character(),
      accommodates = col_skip(),
      bathrooms = col_skip(),
      bathrooms_text = col_skip(),
      bedrooms = col_skip(),
      beds = col_double(),
      amenities = col_skip(),
      price = col_character(),
      minimum_nights = col_skip(),
      maximum_nights = col_skip(),
      minimum_minimum_nights = col_skip(),
      maximum_minimum_nights = col_skip(),
      minimum_maximum_nights = col_skip(),
      maximum_maximum_nights = col_skip(),
      minimum_nights_avg_ntm = col_skip(),
      maximum_nights_avg_ntm = col_skip(),
      calendar_updated = col_skip(),
      has_availability = col_logical(),
      availability_30 = col_double(),
      availability_60 = col_double(),
      availability_90 = col_double(),
      availability_365 = col_double(),
      calendar_last_scraped = col_skip(),
      number_of_reviews = col_double(),
      number_of_reviews_ltm = col_skip(),
      number_of_reviews_l30d = col_skip(),
      first_review = col_skip(),
      last_review = col_skip(),
      review_scores_rating = col_double(),
      review_scores_accuracy = col_skip(),
      review_scores_cleanliness = col_double(),
      review_scores_checkin = col_skip(),
      review_scores_communication = col_skip(),
      review_scores_location = col_double(),
      review_scores_value = col_double(),
      license = col_skip(),
      instant_bookable = col_logical(),
      calculated_host_listings_count = col_skip(),
      calculated_host_listings_count_entire_homes = col_skip(),
      calculated_host_listings_count_private_rooms = col_skip(),
      calculated_host_listings_count_shared_rooms = col_skip(),
      reviews_per_month = col_skip()
    )
  )
}
dir.create(tmp_dir, showWarnings = FALSE, recursive = TRUE)

plan("future::multisession")
data_listings <- data_infos |> filter(basename(url) %in% "listings.csv.gz")
future_mapply(
  FUN = function(url, country, state, city, date, data, basename) {
    filename <- as_filename(url, country, state, city, date)

    if (!file.exists(filename)) {
      message(url)
      dat <- read_listing(url) |>
        add_column(catalog_country = country, catalog_state = state, catalog_city = city, catalog_date = date) |>
        mutate(catalog_date = as.Date(catalog_date))
      arrow::write_parquet(x = dat, sink = filename)
      TRUE
    } else {
      FALSE
    }
  },
  url = data_listings$url,
  country = data_listings$country,
  state = data_listings$state,
  city = data_listings$city,
  date = data_listings$date,
  data = data_listings$data,
  basename = data_listings$basename
)

read_calendar <- function(url) {
  read_csv(
    file = url,
    col_types = cols(
      listing_id = col_character(),
      date = col_date(format = ""),
      available = col_logical(),
      price = col_skip(),
      adjusted_price = col_character(),
      minimum_nights = col_skip(),
      maximum_nights = col_skip()
    )
  )
}
data_calendars <- data_infos |> filter(basename(url) %in% "calendar.csv.gz")
future_mapply(
  FUN = function(url, country, state, city, date, data, basename) {
    filename <- as_filename(url, country, state, city, date)

    if (!file.exists(filename)) {
      message(url)
      dat <- read_calendar(url) |>
        add_column(catalog_country = country, catalog_state = state, catalog_city = city, catalog_date = date) |>
        mutate(catalog_date = as.Date(catalog_date))
      arrow::write_parquet(x = dat, sink = filename)
      TRUE
    } else {
      FALSE
    }
  },
  url = data_calendars$url,
  country = data_calendars$country,
  state = data_calendars$state,
  city = data_calendars$city,
  date = data_calendars$date,
  data = data_calendars$data,
  basename = data_calendars$basename
)
