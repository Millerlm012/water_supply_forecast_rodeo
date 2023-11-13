train_natural_flow <- read.csv("/Users/myasullivan/Downloads/Water_Supply_Forecast_Rodeo_Development_Arena_-_train_monthly_naturalized_flow.csv.csv")

library(dplyr)

## Deal with NAs
# Set NA values to 0 
train_natural_flow[is.na(train_natural_flow)] <- 0

# Create a dataframe including only 0 values for volume 
train_natural_flow_0 <- subset(train_natural_flow, volume == 0)

# Count how many 0s each forecast year has 
where_nas <- table(train_natural_flow_0$forecast_year)
View(where_nas)

# Create a dataframe with only non-zero values for volume 
train_natural_flow_not_0 <- subset(train_natural_flow, volume != 0)
where_not_nas <- table(train_natural_flow_not_0$forecast_year)
View(where_not_nas)

# Merge the counts for NAs and non-NAs together 
merged_nas <- merge(where_nas,
                    where_not_nas,
                    by = "Var1")

# Create a new column with the ratio between NA and non-NA values 
merged_nas$ratio = merged_nas$Freq.x / merged_nas$Freq.y
write.csv(merged_nas, "/Users/myasullivan/Downloads/proj/merged_nas - Sheet1.csv")

# Create another column with the percentage of NA values for each year 
where_nas <- read.csv("/Users/myasullivan/Downloads/proj/merged_nas - Sheet1.csv")
where_nas$percent_na <- where_nas$Freq.x / (where_nas$Freq.x + where_nas$Freq.y)

## Subset data to be after 1928
natural_flow_data <- subset(train_natural_flow, forecast_year >= 1928)

# Define list of site_ids 
site_id_list <- c(unique(natural_flow_data$site_id))

# Replace NA values with averages 
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


