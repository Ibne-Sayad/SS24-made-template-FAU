# Project Plan

## Project Title
Impact of weather on motor vehicle accident in the city of Chicago.

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
This advanced data engineering project aims to analyze the **Impact of weather on motor vehicle accidents in the city of Chicago** generated from several automatic counting stations throughout the city to determine what kind of weather is suitable for driving motor vehicles in the city of Chicago. The project will use two open data sources: [VisualCrossing](https://www.visualcrossing.com), which contains information on weather in Chicago, and [Data.gov](https://data.gov), which provides road accident data of Chicago. The analysis will focus on identifying patterns and trends in accident rates in Chicago throughout the year 2023 to assess the impact of its weather in the city. Additionally, the analysis will examine the weather and climate data to determine if Chicago's weather and climate conditions are conducive for driving in a particular month or year.

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
The analysis of weather and climate conditions, along with road accident in Chicago, can have several significant impacts on the targeted population, including:
1. **Drivers and Passengers:** The analysis can help drivers and passengers to be informed regarding the weather impact of Chicago Highway. They can decide whether it is suitable or not to drive a vehicle in a certain season or month. It can reduce the accident rate of that city in a significant manner. Overall the roads and highway discipline could be controlled through this output.
2. **Tourist Drivers:** The analysis can also benefit the drivers who are driving through this city from a different state or country. Most of the time tourists don't know about the weather conditions of a city, which can cause more accidents on highways. Tourist drivers can make a life-saving decision by getting this kind of analysis. On the other hand, more tourists can visit in risk-free seasons.
3. **Chicago City Planners:** The analysis can also provide insights for city planners to improve the quality and safety of roads and highway facilities in Chicago. By identifying the exact season in which the weather causes the most impact can be easily identified and action can be taken by them using analytics. It will make the city safer for the residents as well as tourists.

Overall, the analysis can help alleviate the pains of uncertainty about the driving conditions in Chicago for drivers, provide insights for city planners to improve the quality and safety of driving facilities in the city, and benefit tourist drivers to make plans to visit Chicago.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Weather Data of New York City
* Metadata URL: [https://www.visualcrossing.com/weather/weather-data-services](https://www.visualcrossing.com/weather/weather-data-services)
* Sample Data URL: [https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=7VFTHZFT2Q26LWQAMFSULD8RS&taskId=51fcebf78072cff4c3ce3a2a01aafc92&zip=false](https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=7VFTHZFT2Q26LWQAMFSULD8RS&taskId=51fcebf78072cff4c3ce3a2a01aafc92&zip=false)
* Data Type: CSV

This data source contains Chicago's weather report generated from [Visual Crossing](https://www.visualcrossing.com) throughout the city from 2023. This data source will provide weather and climate data in New York including air temperature, precipitation, snow depth, visibility, wind speed, and additional relevant attributes. 

### Datasource2: Road Accident Data of the city of Chicago
* Metadata URL: [https://catalog.data.gov/dataset/traffic-crashes-vision-zero-chicago-traffic-fatalities](https://catalog.data.gov/dataset/traffic-crashes-vision-zero-chicago-traffic-fatalities)
* Sample Data URL: [https://data.cityofchicago.org/api/views/gzaz-isa6/rows.csv](https://data.cityofchicago.org/api/views/gzaz-isa6/rows.csv)
* Data Type: CSV

This data source contains Chicago's Road Accident details for the year of 2019 to 2024. There are several attributes of like Accident Date, Location, Victim etc. 

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Extract Data from Multiple Sources [#1][i1]
2. Implement Data Transformation Step in ETL Pipeline [#2][i2]
3. Implement Data Loading Step in ETL Data Pipeline [#3][i3]
4. Automated Tests for the Project [#4][i4]
5. Continuous Integration Pipeline for the Project [#5][i5]
6. Final Report and Presentation Submission [#6][i6]

[i1]: https://github.com/Ibne-Sayad/SS24-made-template-FAU/issues/1
[i2]: https://github.com/Ibne-Sayad/SS24-made-template-FAU/issues/2
[i3]: https://github.com/Ibne-Sayad/SS24-made-template-FAU/issues/3
[i4]: https://github.com/Ibne-Sayad/SS24-made-template-FAU/issues/4
[i5]: https://github.com/Ibne-Sayad/SS24-made-template-FAU/issues/5
[i6]: https://github.com/Ibne-Sayad/SS24-made-template-FAU/issues/6
