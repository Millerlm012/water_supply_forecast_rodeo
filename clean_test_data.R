mydata <- read.csv("/Users/myasullivan/Downloads/Water_Supply_Forecast_Rodeo_Development_Arena_-_train.csv.csv")
metadata <- read.csv("/Users/myasullivan/water_data/water-supply-forecast-rodeo-runtime/data/metadata.csv")

merged_data <- merge(x = mydata, y = metadata, by = "site_id")

sites_to_snotel <- read.csv("/Users/myasullivan/water_data/water-supply-forecast-rodeo-runtime/data/snotel/sites_to_snotel_stations.csv")
station_metadata <- read.csv("/Users/myasullivan/water_data/water-supply-forecast-rodeo-runtime/data/snotel/station_metadata.csv")

snotel_merge <- merge(x = sites_to_snotel, 
                      y = station_metadata, 
                      by = "stationTriplet")

train_natural_flow <- read.csv("/Users/myasullivan/Downloads/Water_Supply_Forecast_Rodeo_Development_Arena_-_train_monthly_naturalized_flow.csv.csv")

library(dplyr)

## Deal with NAs
train_natural_flow[is.na(train_natural_flow)] <- 0
train_natural_flow_0 <- subset(train_natural_flow, volume == 0)
where_nas <- table(train_natural_flow_0$forecast_year)
View(where_nas)
train_natural_flow_not_0 <- subset(train_natural_flow, volume != 0)
where_not_nas <- table(train_natural_flow_not_0$forecast_year)
View(where_not_nas)
merged_nas <- merge(where_nas,
                    where_not_nas,
                    by = "Var1")
merged_nas$ratio = merged_nas$Freq.x / merged_nas$Freq.y
write.csv(merged_nas, "/Users/myasullivan/Downloads/proj/merged_nas - Sheet1.csv")

## Shows ratios of NAs to not NAs 
where_nas <- read.csv("/Users/myasullivan/Downloads/proj/merged_nas - Sheet1.csv")
where_nas$percent_na <- where_nas$Freq.x / (where_nas$Freq.x + where_nas$Freq.y)

## Subset data to be after 1911
natural_flow_data <- subset(train_natural_flow, forecast_year >= 1928)

# Define list of site_ids 
site_id_list <- c(unique(natural_flow_data$site_id))

# Replace NA values for hungry_horse_reservoir_inflow 
for (k in 1:23) {
  for (j in 1:12) {
    for (i in 1:nrow(natural_flow_data)) {
      if (natural_flow_data[i,1] == site_id_list[k] & natural_flow_data[i,4] == j & is.na(natural_flow_data[i,5] == TRUE)) {
        natural_flow_data[i,5] <- with(natural_flow_data, 
                                      mean(volume[month == j & 
                                                  site_id == site_id_list[k]], 
                                         na.rm = TRUE))
      }
    }
  }  
}

# Write new file for the naturalized flow data with the NAs all removed 
write.csv(natural_flow_data, "/Users/myasullivan/Downloads/natural_flow_data_no_nas - Sheet1.csv")

# Every 5 days ? 
start_date = as.Date("2022-10-01")
for (i in 1:59) {
  new_date = as.Date(start_date + (5*i)) 
  print(new_date)
}

# Compare graphs of naturalized flow data with and without NA values 
library(ggplot2)

## READ IN NEW DATAFRAME 
nfd_no_nas <- read.csv("/Users/myasullivan/Downloads/natural_flow_data_no_nas - Sheet1.csv")

# Plot with NAs still in dataset
ggplot(natural_flow_data, aes(x = forecast_year, y = volume))+
  geom_line()+
  facet_wrap(~site_id, scales = "free")

# Plot with NAs removed 
ggplot(nfd_no_nas, aes(x = forecast_year, y = volume))+
  geom_line()+
  facet_wrap(~site_id, scales = "free")


