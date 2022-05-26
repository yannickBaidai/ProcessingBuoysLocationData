#-------------------------------------------------------------------------------------------------#
#                                         BUOY ON BOARD FILTER
#-------------------------------------------------------------------------------------------------#
# Authors :  Yannick BAIDAI,
# Date :     30/10/2018
# From :     Sete
# Comments : Correction function for buoys retrieved by low speed boats (identified in SriLanka
#            zone). it analyzes buoys taht entering in SRI-LANKA zone and distinguishes them from
#            strandings or landings  from boats at low speed. 
#            Apply this function per buoys
#-------------------------------------------------------------------------------------------------#
# inputs :
#-----------#
#    location : dataframe (final filtered buoysDataLocation)
#    buffer   : Spatial buffer around SRI_LANKA Zone, default values corresponds to ZEE area 
#    mapSrc   : Filepath of the sri lanka map (Rdata resources file)
#    speed_threshold : threshold that discriminate buoys at sea and buoys probably on boats in Sri Lanka
#    outlier_speed_threshold : threshold from which a speed is considered as a real outlier on a traj
#    time_threshold : time interval between each speed record for considering them consecutives
#    verbose : show or not processing
#
# outputs :
#-----------#
#   dataframe : buoysDataLocation corrected for sri lanka pattern
#-------------------------------------------------------------------------------------------------#


is_it_close_to_sri_lanka <- function(longitude, latitude,
                                     buffer = 2,
                                     appDir =getwd(),
                                     
                                     plot = F)
{
  #loading maps
  mapSrc = paste0(appDir, "/Resources/Maps/SRIMAP/Sri_Lanka_Map.RData")
  require(rgdal)
  require(rgeos)
  #----Loading data
  if(!exists("sriMap")){
    load(mapSrc, envir = globalenv())
  }
  
  sriMap_buffered <- gBuffer(sriMap, width=buffer, byid = TRUE) #0.18 = 20 km
  
  #---Points close to Sri lanka
  ptsSpDf <- SpatialPoints(data.frame(longitude=longitude, latitude=latitude), proj4string=CRS(proj4string(sriMap_buffered)))
  res <- gContains(sriMap_buffered, ptsSpDf, byid=TRUE)
  
  if(plot ==T){
    plot(sriMap_buffered, lty = 2, axes = T)
    plot(sriMap, add =T)
    points(longitude, latitude, pch =20)
  }
  return(res)
}


sri_lanka_correction <- function(buoy_id,longitude,latitude, timestamp, meanSpeed, buoysOnBoard,
                                 buffer = 1, # about 20 km around Sri lanka island
                                 delta_v_threshold = 2, speed_threshold = 3, time_threshold = 48, 
                                 verbose = T,
                                 ...)
{
  
  df <- data.frame(row = 1:length(longitude),
                   buoy_id = buoy_id,
                   longitude = longitude,
                   latitude = latitude,
                   position_date = timestamp,
                   meanSpeed = meanSpeed,
                   is_onBoardShip = buoysOnBoard) 
  
  #---1. Identifying buoys in sri lanka area
  df$is_closeToSriLanka <- is_it_close_to_sri_lanka(df$longitude, df$latitude,
                                                    buffer = buffer,  plot = F, ...)
  
  #---1. Identifying buoy with transition sea to land  on their trajectories
  #---1.1. Ordering 
  order <- order(df$position_date)
  df_ <- df[order,]
  #---1.2. Land/Sea transition : 
  # 0  : constant location
  # 1  : land -> Sea
  # -1 : Sea -> Land
  # df_$seaToLand <- c(NA, diff(df_$is_onLand))
  df_$seaToSriLanka <- c(NA, diff(df_$is_closeToSriLanka))
  
  #---1.3. Computing speed variation
  df_$delta_v <- c(NA, diff(df_$meanSpeed))
  
  #---2. Isolate period of stay at sea between the land event and the last deployment
  # df_$seaPeriod_bfr_landing <- rep(NA, nrow(df_))
  df_$seaPeriod_bfr_sriLanka <- rep(NA, nrow(df_))
  
  #-No correction applied if no strandings events on the buoy trajectory
  #if(any(df_$seaToLand == 1, na.rm = T))
  if(any(df_$seaToSriLanka == 1, na.rm = T))
  {
    #---Identify sea position between strandings 
    # strandingEvents <- df_$position_date[which(df_$seaToLand == 1)]
    # (Entering in SRI LANKA area)
    strandingEvents <- df_$position_date[which(df_$seaToSriLanka == 1)]
    
    for(i in 1:length(strandingEvents))
    {
      upperLimit <- strandingEvents[i]
      seaPeriod <- subset(df_, position_date < upperLimit & (is_onBoardShip == TRUE| 
                                                               is.na(is_onBoardShip)))$position_date
      
      if(length(seaPeriod)==0){
        #df_$seaPeriod_bfr_landing[which(df_$position_date < upperLimit)] <- i
        df_$seaPeriod_bfr_sriLanka [which(df_$position_date <= upperLimit)] <- i
      }else{
        lowerLimit <-max(seaPeriod)
        # df_$seaPeriod_bfr_landing[which(df_$position_date >= lowerLimit &
        #                                   df_$position_date <= upperLimit)] <- i
        df_$seaPeriod_bfr_sriLanka[which(df_$position_date >= lowerLimit &
                                           df_$position_date <= upperLimit)] <- i
      }
    }
    
    #---3. Analyse of speed profiles on this period
    #---3.1 Treating separately each period of stay at sea on the trajectory
    require(plyr)
    # df_ <- df_[which(!is.na(df_$seaPeriod_bfr_landing) & !is.na(df_$is_onBoardShip)), ]
    df_ <- df_[which(!is.na(df_$seaPeriod_bfr_sriLanka) & !is.na(df_$is_onBoardShip)), ]
    
    if(nrow(df_)>0) # if No period from deploymt to land found, return the "unmodified" data
    {
      # df_ <- ddply(.data= df_, .variables = .(seaPeriod_bfr_landing), .progress = ifelse(verbose, "text", "none"),
      df_ <- ddply(.data= df_, .variables = .(seaPeriod_bfr_sriLanka), .progress = ifelse(verbose, "text", "none"),
                   .fun = function(x)
                   {
                     #---Detecting outliers greater than speed_threshold on speed distribution  
                     # outlier do not belong to [Q1 - 1.5 IQR ; Q3 + 1.5 IQR] , with IQR = Q3 - Q1. 
                     outlier <- boxplot(x$delta_v,  plot= F)$out
                     outlier <- outlier[outlier >= delta_v_threshold]
                     
                     #---Adding also buoys that entering in sri lanka with a high speed (4 knots)
                     # On some trajectories, buoys speed remains high and constant (>4), constute with a
                     # straight line directed towards a fixed zone around sri_lanka (surely buoys on board)
                     highSpeedBuoys <- x$meanSpeed[x$meanSpeed >= speed_threshold]
                     
                     #---checking ff a outlier have been detected
                     if(length(outlier) > 0) # if outliers are detected
                     {
                       
                       outlier.pos   <- which(x$delta_v %in% outlier)
                       outlier.dates <- x$position_date[outlier.pos]
                       outliers.df   <- data.frame(pos = outlier.pos, dates = outlier.dates)
                       outliers.df   <- outliers.df[order(outliers.df$dates),]
                       
                       #--start correction from the last delta_v outlier detected to the end of trjaectories
                       startCorrectionFrom <- outliers.df$pos[1]#[which.max(outliers.df$dates)]
                       endCorrectionAt   <- nrow(x)
                       
                       #---Correction
                       if(verbose){
                         cat("\n+ Correcting assignment for buoy : ", unique(x$buoy_id), "- Sea period :", unique(x$seaPeriod_bfr_sriLanka),"\n")
                         cat("\t- from : ", as.character(x$position_date[startCorrectionFrom]),"\n")
                         cat("\t- to   : ", as.character(x$position_date[endCorrectionAt]),"\n")
                         cat("\t- delta_v outliers :", paste(round(outlier, 2), collapse = "; "), "\n")
                       }
                       x$is_onBoardShip[startCorrectionFrom:endCorrectionAt] <- TRUE
                       
                     }else if(length(highSpeedBuoys)>0)
                     {
                       #---Checking if high speed correspond to consecutives dates
                       outlier.pos <-  which(x$meanSpeed %in% highSpeedBuoys)
                       outlier.dates <- x$position_date[outlier.pos]
                       outliers.df <-  data.frame(pos = outlier.pos, dates = outlier.dates)
                       outliers.df <-  outliers.df[order(outliers.df$dates),]
                       outliers.df$timeDiff <- c(as.numeric(diff.difftime(outliers.df$dates, units= "hours")), NA)
                       
                       #---Suppressing data wtih more than 48h time interval with the next data 
                       outTime_ptr <- which(outliers.df$timeDiff > time_threshold)
                       
                       if(length(outTime_ptr)==0)
                       {
                         #---All data are separated by less than time threshold (consecutive records)
                         #   substitute all states in board from the first speed outlier
                         startCorrectionFrom <- outliers.df$pos[1]
                         endCorrectionAt   <- nrow(x)
                       }else{
                         #---Data are separated by more than time threshold (not consecutive records)
                         #   substitute all states in board from the closest data recorded before entering in sri_lanka
                         startCorrectionFrom <- outliers.df$pos[max(outTime_ptr)]
                         endCorrectionAt   <- nrow(x)
                       }
                       #---Correction
                       if(verbose){
                         cat("\n+ Correcting assignment for buoy : ", unique(x$buoy_id), "- Sea period :", unique(x$seaPeriod_bfr_sriLanka),"\n")
                         cat("\t- from : ", as.character(x$position_date[startCorrectionFrom]),"\n")
                         cat("\t- to   : ", as.character(x$position_date[endCorrectionAt]),"\n")
                         cat("\t- Speed :", paste(round(highSpeedBuoys, 2), collapse = "; "), "\n")
                       }
                       x$is_onBoardShip[startCorrectionFrom:endCorrectionAt] <- TRUE
                     }
                     return(x) 
                   })
      #---Correcting status from df_
      df$is_onBoardShip[match(df_$row, df$row)] <- df_$is_onBoardShip
    }
  }
  return(df)
}

