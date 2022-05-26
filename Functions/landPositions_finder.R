



# f : Full resolution.  These contain the maximum resolution
# of this data and has not been decimated.
# h : High resolution.  The Douglas-Peucker line reduction was
# used to reduce data size by ~80% relative to full.
# i : Intermediate resolution.  The Douglas-Peucker line reduction was
# used to reduce data size by ~80% relative to high.
# l : Low resolution.  The Douglas-Peucker line reduction was
# used to reduce data size by ~80% relative to intermediate.
# c : Crude resolution.  The Douglas-Peucker line reduction was
# used to reduce data size by ~80% relative to low.

loadShoreLine <- function(mapDirs = "D:/RScripts/Echofad_Standard/Resources/Maps/gshhg-shp-2.3.7/GSHHS_shp",
                          resolution = "h", #("f", "h", "i", "l", "c"),
                          units = "deg", # c("meters", "deg")
                          shoreLineBuffer=0.5)
{
  
  gshhs_filePath <- file.path(mapDirs, resolution, "gshhs.RData")
  if(!file.exists(gshhs_filePath))
  {
    # Loading shoreline shapefile (Level 1: Continental land masses and ocean islands, except Antarctica)
    invisible(require(rgdal))
    invisible(require(sp))
    invisible(require(rgeos))
    
    basedir  <- file.path(mapDirs, resolution)
    continental <- paste0("GSHHS_", resolution, "_L1")
    cont_shoreline <- readOGR(dsn = basedir, layer = continental)
    suppressWarnings(cont_shoreline <- gBuffer(cont_shoreline, byid=T, width = 0))
    
    # Loading shoreline shapefile (Level 5: Antarctica based on ice front boundary)
    antartic <- paste0("GSHHS_", resolution, "_L5")
    ant_shoreline <- readOGR(dsn = basedir, layer = antartic)
    # Using a zero-width buffer for cleaning up topology problems in R.
    suppressWarnings(ant_shoreline <- gBuffer(ant_shoreline, byid=T, width = 0))
    
    # Union of antartic and continental masses
    gshhs <- raster::union(cont_shoreline, ant_shoreline)# gUnion(cont_shoreline, ant_shoreline)
    gshhs <- gUnaryUnion(spgeom = gshhs)
    save(gshhs , file= gshhs_filePath)

  }else{
    load(gshhs_filePath)
  }
  
  # Applying buffer
  if (is.numeric(shoreLineBuffer)) {
    # # transform to UTM to have distances in meters
    # if(units == "meter"){
    #   gshhs <- spTransform(gshhs, CRS("+proj=utm +datum=WGS84"))
    # }
    suppressWarnings(gshhs <- rgeos::gBuffer(gshhs, width=shoreLineBuffer))
  }
  return(gshhs)
}


#' Title : loadShoreLine
#'
#' @param appDir : (character) path of folder containing the shoreline maps (default value is the maisn app folder)
#' @param withBuffer    : (logical) apply or not a buffer around shoreline. buffer units is degree
#' @param shoreLineBuffer   : (numeric) value of buffer width (in degree), ignored if withBuffer = FALSE, 
#'
#' @return :  Spatial data.frame of shoreline
#' @details : Loading returning shore line data
# loadShoreLine <- function(appDir = getwd(),
#                           withBuffer=TRUE,
#                           shoreLineBuffer=0.5)
# {
#   invisible(require(sp))
#   invisible(require(rgeos))
#   #shoreLineBuffer = shoreLineBuffer * cos(0)
#   
#   #---load shore line
#   gshhg_file <- paste0(appDir,"/Resources/Maps/GSHHG/gshhs_l.RData")
#   ifelse(file.exists(gshhg_file), invisible(load(gshhg_file)), stop("GSHHG map not found. Operation aborted !\n"))
#   
# 
#   if (withBuffer) {
#     # transform to UTM to have distances in meters
#     # gshhs <- spTransform(gshhs, CRS("+init=epsg:3395"))
#     suppressWarnings(expr = gshhs <- gBuffer(gshhs, width=shoreLineBuffer))
#   }
#   return(gshhs)
# }

#----
#' Title : landPosition.finder
#'
#' @param longitude : (numeric) vector for longitude 
#' @param latitude  : (numeric) vector for latitude 
#' @param useParallel : (logical) use or not parallel computations
#' @param showMap : (logical) show or not density maps and land points
#' @param showStat : (logical) show a quick summary table of processed data
#' @param ... : others arguments accepted by the function loadShoreLine (shoreLineBuffer, withBuffer or appDir)
#' 
#'
#' @return : logical vector, TRUE for position on land.
#' @details : Filtering land points
#' 
#' @examples :
#' setwd("D:/RScripts/Echofad_Standard/")
#' data <-read.csv("Data/All_data/Subsample_ATL_201611.csv")
#' data$land <- landPositions.finder(longitude = data$longitude, latitude = data$latitude, useParallel = T, shoreLineBuffer = 0)
landPositions_finder <- function(longitude, 
                       latitude, 
                       useParallel = FALSE,
                       showMap = T,
                       showStat = T,
                       ...) 
{
  cat("Land positions Finder :\n")
  # Loading shoreline
  cat("\t+ Loading shoreline maps...")
  shoreLine <- loadShoreLine(...)
  cat("Done.\n")
    invisible(require(sp))
    invisible(require(rgeos))
    
  # Filtering Data
  cat("\t+ Searching for land positions...\n")
  ptsSpDf <- SpatialPoints(data.frame(longitude=longitude, latitude=latitude),
                           proj4string=CRS(proj4string(shoreLine)))
  library(foreach)
  if(useParallel == TRUE)
  {
      #---Computations blocks (with parallelisation)
      library(parallel)
      library(doParallel)
      # Defining numbers of clusters to create
      coresNb <- ifelse(detectCores()> 2, detectCores() -2, 2)
      clus <- makeCluster(coresNb)
      registerDoParallel(clus)
  }else
  {
    registerDoSEQ()
  }
  
  # Computations blocks
  subsetSize <- 1000
  breaks <- seq(from=1, to=length(ptsSpDf), by=subsetSize) 
  # progress bar
  require(plyr)
  pb <- txtProgressBar(min=0, max=(length(ptsSpDf)/subsetSize), initial =  0, style = 3)
  library(iterators)
  landPoints <- foreach(x = iter(breaks), 
                        .packages = 'rgeos',
                        .export = 'shoreLine',
                        .combine = 'rbind') %do%
  {
    setTxtProgressBar(pb, (min(x+subsetSize-1, length(ptsSpDf))/subsetSize))
    block <- ptsSpDf[x:min(x+subsetSize-1, length(ptsSpDf)),]
    gContains(shoreLine, block,  byid=T)
  }
  close(pb)
  if(useParallel ==T){
    stopCluster(clus)
  }
  
  
  # Showing optionnal Maps
  if(showMap == TRUE)
    {
      require(ggplot2)
      coords <- data.frame(longitude = longitude, latitude = latitude, landPoints = as.vector(landPoints))
      world <- map_data("world")
      p <- ggplot(data = coords, show.legend = F) +
        # Plotting kernel density maps
        stat_density2d(data = coords,
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
        geom_point(data = subset(coords, landPoints == T),
                   mapping = aes(x=longitude, y=latitude),
                   size = 0.95,
                   shape = 21,
                   fill = "white",
                   color = "red",
                   show.legend = T)+ 
        # Zoom
        coord_cartesian(xlim = c(min(coords$longitude), max(coords$longitude)),
                        ylim = c(min(coords$latitude), max(coords$latitude)))+
        # Labelling
        xlab("longitude")+
        ylab("latitude")+
        labs(title = "Buoys observation density and land points ",
             fill = "Density")+
        theme_minimal()+
        theme(panel.background = element_rect(fill = "white", #"darkblue",
                                              colour = "gray50",
                                              size = 0.5, linetype = "solid"),
              panel.grid.major = element_line(size = 0.5, linetype = 'dashed',
                                              colour = "white"), 
              panel.grid.minor = element_line(size = 0.25, linetype = 'dashed',
                                              colour = "white"))#+
        #coord_fixed(ratio = 1)
      dev.new(); plot(p)
  }
  
  # showing stats
  if(showStat)
  {
    N <- length(latitude)
    tab <- as.data.frame(prop.table(table(landPoints)))
    
    message("- \t ", N, " buoy observation processed :\n")
    print(tab)
  }
  landPoints <- as.vector(landPoints)
  return(landPoints)
}




#' Title : portPositions_finder
#'
#' @param longitude 
#' @param latitude 
#' @param landPositions 
#' @param portBuffer 
#' @param useParallel 
#' @param portName : display or not portName
#' @param showMap 
#' @param appDir 
#' @param ... : arguments for landPositions_finder
#'
#' @return : caharacter value of clossest port
#' @details  : Fonction qui teste la distance d'un point par rapport a une serie d'autres points sr la carte
#'
#' @examples
#' data$portsPositions <- portPositions_finder(longitude = data$longitude, latitude = data$latitude,
#'                         landPositions = data$landPositions, 
#'                         portBuffer = 1,
#'                         useParallel = F,
#'                         portName = T,
#'                         showMap = T)



portPositions_finder <- function(longitude, latitude,
                                  landPositions = NULL, 
                                  portBuffer = 2,
                                  useParallel = FALSE,
                                  portName = F,
                                  showMap = T,
                                  appDir = getwd(),
                                  ...)
{
  
  # If LandPosiotns not provided compute it
  if(is.null(landPositions))
  {
    landPositions <- landPositions.finder(longitude = longitude, latitude = latitude, ...)
  }
  # Creating spDf
  df <- data.frame(row = 1:length(longitude),
                   longitude = longitude,
                   latitude = latitude,
                   landPositions= landPositions)
  if(portName == T)
  {
    df$portsPositions <- rep(NA, nrow(df))
  }else
    {
    df$portsPositions <- rep(FALSE, nrow(df))
  }
  
  
  # Downloading or source port positions
  portFilepath <- paste0(appDir,"/Resources/Maps/World_Ports")
  require(rgdal)
  portsData <- invisible(readOGR(dsn = portFilepath))
  
  # Check if coords is closest to a port for lands points
  landDf <- subset(df, landPositions == T)
  
  if(useParallel == TRUE)
  {
    #---Computations blocks (with parallelisation)
    library(parallel)
    library(doParallel)
    clus <- makeCluster(4)
    registerDoParallel(clus)
  }
  
  require(sp)
  landDf <- plyr::adply(.data = landDf, .margins = 1, .progress = "text", .parallel = useParallel, 
                  .fun = function(x)
                  {
                    # Checking distances between the current point and all ports in data
                    portsData@data$distances <- spDistsN1(portsData,
                                                          SpatialPoints(coords = data.frame(x$longitude, x$latitude)))
                    
                    # Checking proximity with a port in data according to the defind buffer
                    portProximity <- subset(portsData@data, distances <= portBuffer)
                    if(nrow(portProximity)==0)
                    {
                      if(portName == T)
                      {
                        x$portsPositions <- NA
                      }else{
                        x$portsPositions <- FALSE
                      }
                    }else
                      {
                      if(portName == T)
                      {
                        # Find closest port
                        closestPort <- which.min(portProximity$distances)
                        x$portsPositions <- as.character(portProximity$portname[closestPort])
                      }else{
                        x$portsPositions <- TRUE
                      }
                    }
                    return(x)
                  },
                  .paropts = list(.export =c('portBuffer','portsData', 'portName'), .packages= "sp"))
  
  # stop cluster
  if(useParallel == TRUE)
  {stopCluster(clus)}
  
  
  # Showing optionnal Maps
  if(showMap == TRUE)
  {
    require(ggplot2)
    world <- map_data("world")
    landDf$ports <- FALSE
    if(portName == T)
    {
      landDf$ports[which(!is.na(landDf$portsPositions))] <- TRUE
    }else
    {
      landDf$ports[which(landDf$portsPositions == T)] <- TRUE
    }
    p <- ggplot(data = landDf, show.legend = F) +
      # Plotting kernel density maps
      stat_density2d(data = landDf,
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
      geom_point(data = subset(landDf, ports ==T),
                 mapping = aes(x=longitude, y=latitude),# color = ports),
                 size = 1,
                 color = "red",
                 shape = 21,
                 fill = "white",
                 
                 show.legend = T)+ 
      # Zoom
      coord_cartesian(xlim = c(min(landDf$longitude), max(landDf$longitude)),
                      ylim = c(min(landDf$latitude), max(landDf$latitude)))+
      # Labelling
      xlab("longitude")+
      ylab("latitude")+
      labs(title = "Land and Ports buoys",
           fill = "Density",
           color = 'Buoys at port positions')+
      theme_minimal()+
      theme(panel.background = element_rect(fill = "white", #"darkblue",
                                            colour = "gray50",
                                            size = 0.5, linetype = "solid"),
            panel.grid.major = element_line(size = 0.5, linetype = 'dashed',
                                            colour = "white"), 
            panel.grid.minor = element_line(size = 0.25, linetype = 'dashed',
                                            colour = "white"))#+
    #coord_fixed(ratio = 1)
    dev.new(); plot(p)
  }
  
  # returning results
  df$portsPositions[which(df$row %in% landDf$row)] <- landDf$portsPositions
  return(as.vector(df$portsPositions))
}


#' Title : landAndStationnaryPositions_Finder
#'
#' @param longitude 
#' @param latitude 
#' @param landPositions 
#' @param speed.threshold 
#' @param speed 
#' @param showMap 
#' @param ... 
#'
#' @return
#' @export
#'
#' @examples
#' data$stationaryLandBuoys <- landAndStationnaryPositions_Finder(longitude = data$longitude, 
#'                                                             latitude = data$latitude,
#'                                                             landPositions = data$landPositions,
#'                                                             speed = data$velocity,
#'                                                             speed.threshold = 0)
#' 
#' 
#' 
# New version of land_and_stationnary_buoys taking account time window
landAndStationnaryPositions_Finder <- function(longitude, latitude, speed, 
                                               timeWindow = 0, buoy_id = NULL, date =NULL,
                                               landPositions = NULL, 
                                               speed.threshold = 0,
                                               showMap = T,
                                               ...)
{
  # If LandPositions not provided compute it
  if(is.null(landPositions))
  {
    landPositions <- landPositions_finder(longitude = longitude, latitude = latitude, ...)
  }
  
  stationaryLandBuoys <- rep(FALSE, length(speed))
  stationaryLandBuoys[which(landPositions == T & speed <= speed.threshold)] <- TRUE
  
  # Check it into the time window
  if(timeWindow != 0 && !is.null(buoy_id) && !is.null(date))
  {
    df <- data.frame(buoy_id=buoy_id, date = date, speed = speed, land_and_stationary = stationaryLandBuoys)
    true.stationarity <- plyr::adply(.data = df, .margins = 1, .progress ="none", .expand = F, .id = NULL,
                                     .fun = function(x)
                                     {
                                       if(x$land_and_stationary == TRUE)
                                       {
                                         before  <- x$date - lubridate::days(1)
                                         current <- x$date
                                         id <- x$buoy_id
                                         speed.historic <- subset(df, 
                                                                  select = speed,
                                                                  subset = buoy_id == id &
                                                                    (date <= current & date >= before))
                                         speed.historic <- speed.historic[,1]
                                         cat("speed hist :", paste(speed.historic, collapse = ", "), "\n")
                                         if(all(speed.historic <= speed.threshold))
                                         {
                                           return(data.frame(true.stationarity=TRUE))
                                         }
                                       }
                                       return(data.frame(true.stationarity=FALSE))
                                     })
    stationaryLandBuoys <- true.stationarity[,1]
  }else
  {
    cat("Time window not taken into account !\n")
  }
  
  
  # Showing optionnal Maps
  if(showMap == TRUE)
  {
    require(ggplot2)
    world <- map_data("world")
    landDf <- data.frame(longitude = longitude,
                         latitude = latitude,
                         landPositions = landPositions,
                         stationaryLandBuoys = stationaryLandBuoys)
    landDf <- subset(landDf, landPositions == T)
    p <- ggplot(data = landDf, show.legend = F) +
      # Plotting kernel density maps
      stat_density2d(data = landDf,
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
      geom_point(data = subset(landDf, stationaryLandBuoys==T),
                 mapping = aes(x=longitude, y=latitude),
                 size = 1,
                 color = "red",
                 shape = 21,
                 fill = "white",
                 
                 show.legend = T)+ 
      # Zoom
      coord_cartesian(xlim = c(min(landDf$longitude), max(landDf$longitude)),
                      ylim = c(min(landDf$latitude), max(landDf$latitude)))+
      # Labelling
      xlab("longitude")+
      ylab("latitude")+
      labs(title = "Stationary buoys on land",
           fill = "Density",
           color = 'Beached')+
      theme_minimal()+
      theme(panel.background = element_rect(fill = "white", #"darkblue",
                                            colour = "gray50",
                                            size = 0.5, linetype = "solid"),
            panel.grid.major = element_line(size = 0.5, linetype = 'dashed',
                                            colour = "white"), 
            panel.grid.minor = element_line(size = 0.25, linetype = 'dashed',
                                            colour = "white"))#+
    #coord_fixed(ratio = 1)
    dev.new(); plot(p)
  }
  
  return(stationaryLandBuoys)
}
