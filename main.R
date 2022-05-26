# IRD filtering process applied to labelled dataset (25-02/2019 received from J. Uranga)
# Author : Y.Baidai & M. Capello
# Place : Sete
# Date : 26-02-2019
# e-mail : yannick.baidai@gmail.com



rm(list = ls())
wd <- "D:/Research Projects/RECOLAPE/6-New dataset RECOLAPE-25-02-2019"
setwd(wd)

fn <- file.path(wd, "Data", "testFile.csv")


# 1-loading datafiles
library(dplyr)
status <- read.csv2(fn, stringsAsFactors = F)%>%
  dplyr::mutate(TIMESTAMP = as.POSIXct(TIMESTAMP, 
                                       tz="GMT",
                                       tryFormats = c("%Y-%m-%d %H:%M",
                                                      "%Y/%m/%d %H:%M",
                                                      "%d-%m-%Y %H:%M",
                                                      "%d/%m/%Y %H:%M"),
                                       usetz = F))


# 1. Land Filtering --------------------------------------------------------
source("Functions/landPositions_finder.R")
status$landPositions <- landPositions_finder(longitude = status$LONGITUDE,
                                             latitude = status$LATITUDE,
                                              useParallel = T, 
                                              showMap = T,
                                              showStat = T,
                                              shoreLineBuffer = 0.05,
                                              resolution ="l") 


# 2. Duplicated positions Filtering -----------------------------------------
source("Functions/duplicatedData_finder.R")
status$duplicated <- duplicatedData_finder(buoyId = status$ID, 
                                           positionDate = status$TIMESTAMP,
                                           latitude =  status$LATITUDE, 
                                           longitude = status$LONGITUDE)


# 3. Ubiquitous observations filtering --------------------------------------
# (for same date-time two different positions)
source("Functions/ubiquitousData_finder.R")
status$ubiquitous <- ubiquitousData_finder(buoyId = status$ID,
                                           positionDate = status$TIMESTAMP,
                                           longitude =  status$LONGITUDE,
                                           latitude = status$LATITUDE,
                                           maxDistInKilometers=1,
                                           showMap = T,
                                           showStat = T)


# 4. IsoLATITUDEed observations filtering and computing trajID -------------------
#' @note_on_T7_BUOYS : some ES buoys (e.g. "T7+003161592") have two positions for the same date
#' All their data were labelled as ubiquitous and retrieved from the dataset for future calcuLATITUDEions.
source("Functions/isolatedPositions_finder.R")
status <- cbind.data.frame(status, 
                           isolatedPositions_finder(buoyId = status$ID,
                                                    positionDate = status$TIMESTAMP,
                                                    longitude = status$LONGITUDE,
                                                    latitude = status$LATITUDE, 
                                                    ubiquitousData = status$ubiquitous,
                                                    duplicatedData = status$duplicated,
                                                    single.threshold = 1, 
                                                    timeIntervalThreshold=48, 
                                                    speedThreshold=35,
                                                    showMap = T, showStat = T))


# 5. Buoys on board filtering ------------------------------------------------
source("Functions/buoysOnBoard_finder.R")
status$buoysOnBoard <- buoysOnBoard_finder(buoy_id = status$ID, 
                                           trajId = status$trajId,
                                           timestamp = status$TIMESTAMP,
                                           longitude = status$LONGITUDE,
                                           latitude = status$LATITUDE,
                                           isolatedPositions = status$isolatedPositions,
                                           TIME_PERIOD_HISTORY = 3, 
                                           MAX_DRIFTING_SPEED = 6,
                                           transitionDuration = 24, 
                                           forcedAssignment = T,
                                           useParallel = F)


# 6. Buoys on port filtering -----------------------------------------------------------
status$portPositions <-  portPositions_finder(longitude = status$LONGITUDE,
                                              latitude = status$LATITUDE,
                                              landPositions = status$landPositions,
                                              portBuffer = 2, # 2km Buffer around port positions
                                              portName = T, # Display closest port name or NA (if no closest port)
                                              showMap = T)

# 7. Stationary buoys on land -----------------------------------------------------------
status$meanSpeed <- computeLocationDataMeanSpeed(buoy_id = status$ID,
                                                 trajId = status$trajId,
                                                 longitude = status$LONGITUDE,
                                                 latitude = status$LATITUDE,
                                                 time = status$TIMESTAMP)

status$stationaryLandBuoys <- landAndStationnaryPositions_Finder(longitude = status$LONGITUDE,
                                                                 latitude = status$LATITUDE,
                                                                  landPositions = status$landPositions,
                                                                  speed = status$meanSpeed,
                                                                  speed.threshold = 0.01) # Speed of stationnary buoys

#note: for beaching events need to discard ports  : stationatyLandBuoys-portPos

##### UPDATE  17/09/2018 : Column fields buoysStatus and trajId correction ########################################
###################################################################################################################
source("Functions/set_buoysStatus.R")
status$buoyStatus <- set_buoysStatus(landPositions = status$landPositions,
                                      duplicated = status$duplicated,
                                      ubiquitous = status$ubiquitous,
                                      buoysOnBoard = status$buoysOnBoard,
                                      portPositions = status$portPositions,
                                      stationaryLandBuoys = status$stationaryLandBuoys,
                                      isolatedPositions = status$isolatedPositions)

status$trajId2 <- set_StatusTrajId(buoy_id = status$ID,
                                    trajId  = status$trajId,
                                    position_date = status$TIMESTAMP,
                                    status = status$buoyStatus)

