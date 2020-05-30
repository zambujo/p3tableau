# preprocessing p3.snf.ch data for visualization

library(tidyverse)
library(lubridate)
library(janitor)
library(glue)
library(jsonlite)
library(here)
library(conflicted)
conflict_prefer("filter", "dplyr")
conflict_prefer("lag", "dplyr")

# ---------------------------------------------

# adding geo coordinates to HEU
nominatim_api <- function(query,
                          city, 
                          iso2c, 
                          email,
                          class = "amenity",
                          type = "university",
                          limit = 1,
                          debug = 0) {
  Sys.sleep(1) # becaue of API limits
  glue("https://nominatim.openstreetmap.org/",
       "search.php?city={ city }&",
       "countrycodes={ iso2c }&",
       "email={ email }&",
       "class={ class }&",
       "type={ type }&",
       "limit={ limit }&",
       "debug={ debug }&",
       "format=json&",
       "q={ query }") %>%
    fromJSON() %>%
    select(place_id,
           osm_type,
           lat,
           lon,
           display_name,
           class,
           type,
           importance)
}

# ---------------------------------------------

# download data from p3.snf.ch and load it to R
projects <- read_csv2("raw_data/P3_GrantExport.csv",
                      guess_max = 50000) # 39 MB
people <- read_csv2("raw_data/P3_PersonExport.csv",
                    guess_max = 50000) # 12 MB

# make column names snake case
projects <- clean_names(projects)
people <- clean_names(people)

# institutions of interest: swiss univesities
HEU <- tibble(
  uni2c = c("ZH", "GE", "ETHZ", "BE", "BS", "EPFL", 
          "LA", "FR", "NE", "SG", "LU", "USI"),
  # also known as
  aka = c("UZH", "Uni Bastions", "ETH Zentrum", 
          "Bern University", "Basel University", "EPFL",
          "UNIL", "Fribourg University", "UNINE", 
          "HSG", "Lucerne University", "USI"),
  city = c("Zurich", "Geneva", "Zurich", "Bern", 
           "Basel", "Lausanne", "Lausanne", "Fribourg", 
           "Neuchatel", "Sankt Gallen", "Lucerne", "Lugano"),
)

heu_projects <- 
  projects %>%
  select(grant_number = project_number,
         scheme = funding_instrument_hierarchy,
         domain = discipline_name_hierarchy,
         university,
         start_date,
         end_date,
         approved_amount) %>%
  mutate(
    uni2c = map_chr(str_split(university, " - "), tail, n = 1),
    university = map_chr(str_split(university, " - "), head, n = 1)) %>%
  semi_join(HEU, by = "uni2c") %>%
  filter(!is.na(domain), !is.na(scheme)) %>% # ad-hoc
  mutate(
    grant_number = as.integer(grant_number),
    approved_amount = as.numeric(approved_amount),
    start_date = dmy(start_date),
    end_date = dmy(end_date),
    domain = map_chr(str_split(domain, ";"), head, n = 1),
    scheme = map_chr(str_split(scheme, ";"), head, n = 1))

api_results <- map2_df(
  pull(HEU, aka), 
  pull(HEU, city), 
  nominatim_api, 
  iso2c = "ch", 
  email = "joao@sciencegeist.ch")

uni_coord <- tibble(
  uni2c = pull(HEU, uni2c),
  lat = as.double(pull(api_results, lat)),
  lon = as.double(pull(api_results, lon))
)

grantees <- people %>%
  select(gender, 
         person_id = person_id_snsf,
         proj_ma_app = projects_as_responsible_applicant,
         proj_co_app = projects_as_applicant) %>%
  # at least a project as main or co-applicant
  filter(!is.na(proj_ma_app) | !is.na(proj_co_app)) %>%
  unite("grant_number", starts_with("proj_"), sep = ";", na.rm = TRUE) %>%
  mutate(
    gender = str_sub(gender, 1, 1),
    person_id = as.integer(person_id),
    grant_number = str_split(grant_number, ";"),
    grant_number = map(grant_number, as.integer)) %>%
  unnest(grant_number) %>%
  distinct(gender, person_id, grant_number)

# number of main and co-applicants per project
pplpgrant <- count(grantees, grant_number)

p2 <- grantees %>% 
  inner_join(heu_projects, by = "grant_number") %>%
  inner_join(uni_coord, by = "uni2c") %>%
  inner_join(pplpgrant, by = "grant_number") %>%
  arrange(grant_number) %>%
  mutate(
    year_start = round_date(start_date, unit = "year"),
    year_end = round_date(end_date, unit = "year"),
    n_years = interval(year_start, year_end) %/% years(1),
    # 1 year default minimum
    n_years = ifelse(n_years == 0, 1, n_years),
    # amount per applicant
    amount_pp = approved_amount / n,
    # amount per applicant per year
    amount = amount_pp / n_years) %>%
  filter(!is.na(n_years)) %>%
  mutate(year = map2(year_start, year_end,
                      function(x, y) head(year(x):year(y), -1))) %>%
  select(-approved_amount, 
         -amount_pp,
         -n, 
         -n_years, 
         -starts_with("year_"),
         -ends_with("_date"))

p2 %>%
  unnest(year) %>%
  save(file = "data/p2.R")
# write_csv(path = "data/p2.csv")

