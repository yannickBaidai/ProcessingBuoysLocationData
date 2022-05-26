#----------------------------------------------------------------------------------------------#
#                                    singleData_filter
#----------------------------------------------------------------------------------------------#
# Author : 
# Date :
# From :
# Comments : 
#----------------------------------------------------------------------------------------------#
findIdenticalTimeStamp <- function(timeStamp, longitude, latitude, maxDistInKilometers=1) {
  
  good <- rep(TRUE, times=length(timeStamp))
  dup <- duplicated(timeStamp)
  if (sum(dup) > 0) {
    require(sp)
    
    timeStampDup <- timeStamp[dup]
    dup <- lapply(timeStampDup, function(x) which(timeStamp %in% x))
    keep <- lapply(dup, function(x) {
      ptsDf <- data.frame(longitude=longitude[x], latitude=latitude[x])
      coordinates(ptsDf) <- ~longitude+latitude
      proj4string(ptsDf) = CRS("+init=epsg:4326")
      # transform to UTM to have distances in meters
      ptsDf <- spTransform(ptsDf, CRS("+init=epsg:3395"))
      if (all(spDists(ptsDf)/1000 < maxDistInKilometers)) {
        # ok, all points are close, we keep the first
        return(x[1])
      } else {
        # no, we discard all pts
        return(NULL)
      }
    })
    keep <- unlist(keep)
    toRemove <- setdiff(unlist(dup), keep)
    good[toRemove] <- FALSE
  }
  return(good)
}

# --------------------------------------------------------------------------------------------------------------
#' @Title : filterNoIdenticals
# --------------------------------------------------------------------------------------------------------------
#' @param maxDistInKilometers : refers to function "findIdenticalTimeStamp"
#'
#' @param useParallel : Wether or not use parallel computations
#' @param buoyId : character vector of buoy code
#' @param positionDate : date, posixct vector of observation timestamp
#' @param longitude : numeric vector of longitude
#' @param latitude : numeric vector of latitude
#'
#' @return : logical vector, ubiqutous data = TRUE
#' @details : application of the previous function to a whole set of buoys
filterNoIdenticals <- function(buoyId,
                               positionDate,
                               longitude,
                               latitude,
                               maxDistInKilometers=1) 
{
  # dataframe
  df <- data.frame(data_index=1:length(buoyId),
                   buoy_id = buoyId,
                   position_date = positionDate,
                   latitude = latitude,
                   longitude = longitude,
                   stringsAsFactors = F)
  #---Computations blocks (with parallelisation)
  require(plyr)
  res <- ddply(.data=cbind(df, data_index=1:nrow(df)), .variables=.(buoy_id),.progress = "text", 
               .fun=function(subDf)
               {
                 data.frame(value=findIdenticalTimeStamp(timeStamp=subDf$position_date, 
                                                         longitude=subDf$longitude, 
                                                         latitude=subDf$latitude, 
                                                         maxDistInKilometers=maxDistInKilometers),
                            data_index=subDf$data_index)
               })
  
  # Ordering (initial data order) and returning data
  return(!res$value[order(res$data_index)])
}

# --------------------------------------------------------------------------------------------------------------
#' Title : ubiquitousData_finder
# --------------------------------------------------------------------------------------------------------------
#' @param buoyId 
#' @param buoyId : character vector of buoy code
#' @param positionDate : date, posixct vector of observation timestamp
#' @param longitude : numeric vector of longitude
#' @param latitude : numeric vector of latitude
#' @param useParallel : Wether or not use parallel computations
#' @param showMap : show map of ubiquitous point found
#' @param showStat : show stat of ubiquitous point found
#' @param ... : arguments for function filterNoIdentical : maxdistKilometer set 1 km as default value
#'
#' @return : logical vector, ubiqutous data = TRUE
#'
#' @examples
#' d <- ubiquitousData_finder(buoyId,
#'                            positionDate,
#'                            latitude,
#'                            longitude,
#'                            useParallel = FALSE,
#'                            showMap = T,
#'                            showStat = T)
ubiquitousData_finder <-   function(buoyId,
                                    positionDate,
                                    latitude,
                                    longitude,
                                    showMap = T,
                                    showStat = T,
                                    ...)
{
  cat("Ubiquitous data finder :\n")
  data <- data.frame(buoy_id = buoyId,
                     position_date = positionDate,
                     latitude = latitude,
                     longitude = longitude,
                     stringsAsFactors = F)
  
  # Find observations with same timestamp and differents positions
  data$ubiquitousData <- filterNoIdenticals(buoyId = buoyId,
                                            positionDate = positionDate,
                                            latitude = latitude,
                                            longitude = longitude,
                                            ...)
  
  # Showing optionnal Maps
  if(showMap == TRUE)
  {
    if(is.null(longitude) || is.null(latitude))
    {
      warning("No coordinates provided for maps.\n")
    }else
    {
      if(any(data$ubiquitousData == T)){
        require(ggplot2)
        world <- map_data("world")
        p <- ggplot(data = data, show.legend = F) +
          # Plotting kernel density maps
          stat_density2d(data = data,
                         mapping = aes(x =longitude, y=latitude, fill = ..level..),
                         geom="polygon",
                         contour = T,
                         show.legend = T)+
          scale_fill_gradient(low=  "lightblue",
                              high= "darkblue")+
          # World map plotting
          geom_map(data=world,
                   map=world,
                   aes(map_id=region),
                   color="white", fill="grey", size=0.05, alpha=1)+
          # overplotting points
          geom_point(data = subset(data, ubiquitousData == T),
                     mapping = aes(x=longitude, y=latitude),
                     size = 2,
                     shape = 21,
                     fill = "white",
                     #color = "red",
                     show.legend = F)+ 
          geom_text(data = subset(data, ubiquitousData == T),
                    mapping = aes(x=longitude, 
                                  y=latitude, 
                                  label = paste(buoy_id,"\n", position_date),
                                  angle = 0, vjust = "top" , hjust = "right"),
                    size = 2.5,
                    show.legend = FALSE,
                    check_overlap = T)+
          # Zoom
          coord_cartesian(xlim = c(min(data$longitude), max(data$longitude)),
                          ylim = c(min(data$latitude), max(data$latitude)))+
          # Labelling
          xlab("longitude")+
          ylab("latitude")+
          labs(title = "Ubiquitous data",
               fill = "Density")+
          theme_minimal()+
          theme(panel.background = element_rect(fill = "white", #"darkblue",
                                                colour = "gray50",
                                                size = 0.5, linetype = "solid"),
                panel.grid.major = element_line(size = 0.5, linetype = 'dashed',
                                                colour = "white"), 
                panel.grid.minor = element_line(size = 0.25, linetype = 'dashed',
                                                colour = "white"))
        dev.new(); plot(p)
      }
      
    }
  }
  
  # showing stats
  if(showStat)
  {
    N <- nrow(data)
    tab <- as.data.frame(prop.table(table(data[,'ubiquitousData'])))
    names(tab) <- c("ubiquitousData", "Freq")
    print(tab)
  }
  
  return(data$ubiquitousData)
}
duplicatedData_finder <-  function(buoyId,
                                   positionDate,
                                   latitude,
                                   longitude,
                                   showStat = T)
{
  cat("Duplicated data  Finder :\n")
  data <- data.frame(buoy_id = buoyId,
                     position_date = positionDate,
                     latitude = latitude,
                     longitude = longitude,
                     stringsAsFactors = F)
  duplicated <- duplicated(data)
  
  if(showStat == T)
  {
    N <- length(buoyId)
    tab <- as.data.frame(prop.table(table(duplicated)))
    
    message("- \t ", N, " buoy observation processed :\n")
    print(tab)
  }
  return(duplicated)
}
#' Title ; temporalDistance
#'
#' @param time 
#' @param units 
#'
#' @return
#' @export
#'
#' @examples
temporalDistance <- function(time, units="hours"){
  as.numeric(diff(time), units=units)
}

#' Title : geographicalDistance
#'
#' @param longitude 
#' @param latitude 
#'
#' @return
#' @export
#'
#' @examples
geographicalDistance <- function(longitude, latitude) {
  require(sp)
  spDists(cbind(longitude, latitude), segments = TRUE, longlat = TRUE)
}

#' Title : simpleSpeed
#'
#' @param longitude 
#' @param latitude 
#' @param time 
#' @param includeFirst 
#' @param order 
#'
#' @return
#' @export
#'
#' @examples
simpleSpeed <- function(longitude, latitude, time, includeFirst=TRUE, order=FALSE) {
  
  if (order) {
    orderInd <- order(time)
    longitude <- longitude[orderInd]
    latitude <- latitude[orderInd]
    time <- time[orderInd]
  }
  
  distTime <- temporalDistance(time=time, units="hours")
  distGeom <- geographicalDistance(longitude, latitude)
  
  if (includeFirst) {
    return( c(NA, distGeom/distTime))
  } else {
    return(distGeom/distTime)
  }
}

#' Title
#'
#' @param timeStamp 
#' @param longitude 
#' @param latitude 
#' @param timeIntervalThreshold 
#' @param speedThreshold 
#'
#' @return
#' @export
#'
#' @examples
cutTrajectory <- function(time, 
                          longitude,
                          latitude,
                          timeIntervalThreshold=48,
                          speedThreshold=35) 
{
  if (length(time) == 1) {
    return(1)
  }
  
  timeDiff <- temporalDistance(time=time, units="hours")
  speed <- simpleSpeed(longitude=longitude, latitude=latitude, time=time, includeFirst=FALSE)
  speed[is.na(speed) | is.infinite(speed)] <- speedThreshold + 1
  change <- timeDiff >= timeIntervalThreshold | speed >= speedThreshold
  return(cumsum(c(TRUE, change)))
}



#' Title
#'
#' @param buoyId : (character) vector of Code or Id of buoy observations
#' @param single.threshold : (numeric) threshold value for considering data a s single (default = 1)
#' @param useParallel : (logical) use parallel computation
#' @param showMap : (logical) show or not density maps and land points
#' @param showStat : (logical) show a quick summary table of processed data
#' @param positionDate 
#' @param longitude 
#' @param latitude 
#' @param ... : timeIntervalThreshold and speedThreshold for function cutTrajectory
#'
#' @return : logical vector, TRUE for single data detected.
#'
#' @examples
#' 
#' setwd("D:/RScripts/Echofad_Standard/")
#' buoysdata <- read.csv("Data/All_data/Subsample_ATL_201611.csv")
#' data$single <- isolatedPositions_finder(buoyId = data$code, 
#'                                   position_date = data$date
#'                                   longitude = data$longitude, latitude = data$latitude, 
#'                                   useParallel = T, showMap =T)
#' 
isolatedPositions_finder <- function(buoyId, 
                                     positionDate,
                                     longitude,
                                     latitude,
                                     ubiquitousData =NULL,
                                     duplicatedData =NULL,
                                     single.threshold = 1,
                                     showMap = T,
                                     showStat = T,
                                     timeIntervalThreshold=48,
                                     speedThreshold=35)
{
  
  
  # Seeking for ubiquitous or duplicated(if values not provided)
  if(is.null(ubiquitousData)){
    cat("Seeking for ubiquitous data\n")
    ubiquitousData <- ubiquitousData_finder(buoyId = buoyId, positionDate = positionDate,latitude = latitude, longitude = longitude, showMap = F, showStat = F)
  }
  if(is.null(duplicatedData)){
    cat("Seeking for duplicated data\n")
    duplicatedData <- duplicatedData_finder(buoyId = buoyId, positionDate = positionDate,latitude = latitude, longitude = longitude, showStat = F)
  }
  cat("Single data  Finder :\n")
  # Compute trajId : cut trajectory from temporal gap or spedd anomaly within buoy tracks
  require(plyr)
  cat("\t+ Cutting buoy tracks...\n")
  data <- data.frame(row = 1:length(buoyId), 
                     time = as.POSIXct(positionDate, tz = 'UTC'),
                     buoyId = as.character(buoyId), 
                     longitude = longitude,
                     latitude = latitude,
                     ubiquitous = ubiquitousData,
                     duplicated= duplicatedData,
                     stringsAsFactors = F)
  data <- ddply(.data = data, .variables = .(buoyId), .progress = "text", 
                       .fun = function(df)
                      {
                        # retrieve ubiquitous and duplicated from trajId calc
                        df$trajId <- NA
                        df_ <- subset(df, ubiquitous == F &
                                          duplicated == F)
                        if(nrow(df_)>0)
                        {
                          df_ <- df_[order(df_$time), ]
                          df_$trajId <- cutTrajectory(time = df_$time,
                                                     longitude = df_$longitude,
                                                     latitude = df_$latitude,
                                                     timeIntervalThreshold=timeIntervalThreshold,
                                                     speedThreshold=speedThreshold)
                          # Asssign trajID for valid data
                          df$trajId[which(df$row %in% df_$row)] <- df_$trajId
                        }
                        return(df)
                      })
  
  # Processing data
  cat("\t+ Searching for isolated buoy observation on data...\n")
  data <- ddply(.data = data, .variables = .(buoyId, trajId),.progress = "text",
               .fun= function(x)
               {
                 currentTraj <- unique(x$trajId)
                 if(is.na(currentTraj))
                 {
                   single <- NA
                 }else
                   {
                   if(nrow(x) > single.threshold)
                     {
                     single <- FALSE
                   }else{
                     single <- TRUE
                   }
                 }
                 
                 return(cbind.data.frame(x, isolatedPositions =rep(single, nrow(x))))
               })

  
  # Showing optionnal Maps
  if(showMap == TRUE && any(data$isolatedPositions == TRUE))
  {
    if(is.null(longitude) || is.null(latitude))
    {
         warning("No coordinates provided for maps.\n")
    }else
    {
      require(ggplot2)
      world <- map_data("world")
      p <- ggplot(data = data, show.legend = F) +
        # Plotting kernel density maps
        stat_density2d(data = data,
                       mapping = aes(x =longitude, y=latitude, fill = ..level..),
                       geom="polygon",
                       contour = T,
                       show.legend = T)+
        scale_fill_gradient(low=  "lightblue",
                            high= "darkblue")+
        # World map plotting
        geom_map(data=world,
                 map=world,
                 aes(map_id=region),
                 color="white", fill="grey", size=0.05, alpha=1)+
        # Overplotting points
        geom_point(data = subset(data, isolatedPositions == T),
                   mapping = aes(x=longitude, y=latitude),
                   size = 1.5,
                   shape = 21,
                   fill = "white",
                   color = "red",
                   show.legend = F)+ 
        geom_text(data = subset(data, isolatedPositions == T),
                  mapping = aes(x=longitude, 
                                y=latitude, 
                                label = buoyId,
                                angle = 0, vjust = "top" , hjust = "right"),
                  size = 2.8,
                  color = "red",
                  show.legend = F,
                  check_overlap = T)+
        # Zoom
        coord_cartesian(xlim = c(min(data$longitude), max(data$longitude)),
                        ylim = c(min(data$latitude), max(data$latitude)))+
        # Labelling
        xlab("longitude")+
        ylab("latitude")+
        labs(title = "Buoys observation density and isolated points ",
             fill = "Density")+
        theme_minimal()+
        theme(panel.background = element_rect(fill = "white", #"darkblue",
                                              colour = "gray50",
                                              size = 0.5, linetype = "solid"),
              panel.grid.major = element_line(size = 0.5, linetype = 'dashed',
                                              colour = "white"), 
              panel.grid.minor = element_line(size = 0.25, linetype = 'dashed',
                                              colour = "white"))
      dev.new(); plot(p)
    }
  }
  
  # showing stats
  if(showStat)
  {
    N <- nrow(data)
    tab <- as.data.frame(prop.table(table(data[,'isolatedPositions'], useNA ="ifany")))
    names(tab)[1] <- c("isolatedPositions")
    message("- \t ", N, " buoy observation processed :\n")
    message("NA values correspond to ubiquitous or duplicated data \n.")
    print(tab)
  }
  
  # Ordering and returning results
  data <- data[order(data$row), c("trajId", "isolatedPositions")]
  return(data)
}

