#---DUPLICATED POSITIONS FOR SAME TIMESTAMP FILTER

# --------------------------------------------------------------------------------------------------------------
#' @Title : findIdenticalTimeStamp 
# --------------------------------------------------------------------------------------------------------------
#' @param timeStamp : (Posixct, date) vector of position dates
#' @param longitude : (numeric) vector
#' @param latitude  : (numeric) vector
#' @param maxDistInKilometers (numeric) value of max distance between two duplicated points
#'
#' @return : logical vector, TRUE = duplicated timestamp) with two differents positions
#' @details : find for a single buoy, "ubiquitous" observations (duplicated date with two different positions) 
#'            if positions are separated by less than the value defined by maxDistInKilometers,
#'            keep the first as the valid one and dsicards the other (FALSE)
#'            else all positions are discarded
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
