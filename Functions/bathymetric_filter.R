#                                                     BATHYMETRIC_FILTER FUNCTIONS
#----------------------------------------------------------------------------------------------------------------------------------#
# Author : Yannick BAIDAI
# Date :
# From :
# Comments : Calcule la profondeur (altitude) associée a des positions, ainsi que les profondeurs (altitudes) max
#            et min dans la fenetre spatiale definie autour des points
#---# inputs : latitude = vector(numeric)
#              longitude = vector(numeric)
#              windowsRange = rayon de la fenetre spatiale autour du point (en deg) : (0.033 = 2 min) < windowsRange < 1° (3 et 111 km)
#              bathyData_file = file resources for bathymetric data, if not provided download from NOAA server
#---# outputs : depthData = data.frame (depth and optionally min_depth, max_depth)    
#----------------------------------------------------------------------------------------------------------------------------------#


#----LOAD BATHY DATA (and return a raster 'bathyData')
get_bathymetricRessources<-function(maxLon = -1, maxLat = -1, bathymetricData = NULL){
	invisible(require(sp))
	invisible(require(marmap))
	invisible(require(raster))
	
	#---1 arc minute resolution ETOPO bathymetric maps
	  if(!is.null(bathymetricData) && file.exists(bathymetricData)) 
	  {
		bathy <- brick(bathymetricData) 
		projection(bathy) <- CRS(projargs="+init=epsg:4326 +proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")
		
	  }else#--Warning message (downloading data from NOAA server)
	  {
		if(maxLon == -1 || maxLat == -1){
			stop("Invalid map box !")
		}else
		{
			cat("No valid bathymetric data files found. Downloading data from NOAA servers...\n")
			latMax <- max(lat) + 2
			latMin <- min(lat) - 2
			lonMax <- max(lon) + 2
			lonMin <- min(lon) - 2
			
			res <- 1
			bathy <- NA
			
			repeat
			{
			  if(res > 5)
			  {
				stop("Maximum download attempts reached : Data servers probably down !\n")
				break
			  }
			  cat(paste0("Bathymetric data downloading. Attempt No", res, "...\n"))
			  tryCatch(bathy <- getNOAA.bathy(lon1 = lonMin, lon2 = lonMax, lat1 = latMin, lat2 = latMax, resolution = res, keep = TRUE, antimeridian = FALSE),
					   error = function(err) 
					   {
						 warning("NOAA servers cannot be reached")
						 cat("Trying to download data with a lowered resolution.")
					   }, 
					   warning = function(war)
					   {
						 cat("Trying to download data with a lowered resolution.")
						 
					   }, silent=TRUE)
			  
			  res <- res + 1
			  if(is.bathy(bathy))
			  {
				bathy <- marmap::as.raster(bathy)
				projection(bathy) <- CRS(projargs="+init=epsg:4326 +proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")
				break
			  }
			}
		}		
	  }	
	return(bathy)
}


#----BATHYMETRIC FILTER
get_depthData <- function(lon, lat, windowsRange = 0, bathy = bathyData, useParallel = FALSE)
{
  require(sp)
  require(marmap)
  require(raster)
  
  #---Return data UseMaxArea or not
  depthData <- raster::extract(bathy, data.frame(x=lon, y=lat))
  depthData <- as.data.frame(depthData)
  colnames(depthData) <- "depth"
  
  if(windowsRange > 0)
  {
    maxDepthData <- raster::extract(bathy, data.frame(x=lon, y=lat), buffer = (windowsRange *1000))
    maxDepthData <- ldply(maxDepthData, .fun = max, .progress = "text", .parallel = useParallel)
    maxDepthData <- as.data.frame(maxDepthData)
    colnames(maxDepthData) <- "max_depth"
    depthData <- cbind(depthData, maxDepthData)
  }else if(windowsRange < 0)
  {
    warning("Invalid parameter \'windowsRange\' (windows range must be between 0.0333 and 1) !")
  }
  
  return(depthData)
}

#----BATHYMETRIC FILTER
getShallowPoints <- function(df, depthLimit = (-150), useMaxDepth = FALSE)
{
  depth <- rep_len(NA, nrow(df))
  ifelse((useMaxDepth == TRUE), depth <- df$max_depth, depth <- df$depth)
  shallowPoints <- which(df$depth > depthLimit)

  return(shallowPoints)
}