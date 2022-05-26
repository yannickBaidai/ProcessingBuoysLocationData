


#' Title
#'
#' @param land 
#' @param duplicated 
#' @param ubiquitous 
#' @param onBoard 
#' @param portPositions 
#' @param stationaryLandBuoys 
#' @param isolated 
#'
#' @return
#' @export
#'
#' @examples
#' status <- set_buoysStatus(landPositions = data_FR$landPositions,
#'                           duplicated = data_FR$duplicated,
#'                           ubiquitous = data_FR$ubiquitous,
#'                           buoysOnBoard = data_FR$buoysOnBoard,
#'                           portPositions = data_FR$portPositions,
#'                           stationaryLandBuoys = data_FR$stationaryLandBuoys,
#'                           isolatedPositions = data_FR$isolatedPositions)
#' 
set_buoysStatus <- function(landPositions,  portPositions, stationaryLandBuoys, 
                            duplicated, ubiquitous,isolatedPositions,
                            buoysOnBoard)
{
  status <- rep(NA, length(landPositions))
  portPositions <- ifelse(is.na(portPositions), FALSE, TRUE)
  
  # Adding a new fields status for mapping purpose
  # status[which(buoysOnBoard == TRUE)]  <-  "board"
  # status[which(buoysOnBoard == FALSE)] <-  "water"
  # status[which(is.na(buoysOnBoard))]   <- "not_classified"
  # status[which(landPositions == TRUE)] <-  "land"
  # status[which(stationaryLandBuoys == TRUE)] <- "stationnary"
  # status[which(portPositions == TRUE)] <-  "port"
  # status[which(isolatedPositions == T)] <- "isolated"
  # status[which(duplicated == TRUE)] <- "duplicated"
  # status[which(ubiquitous == TRUE)] <- "ubiquitous"
  status[which(buoysOnBoard == TRUE)]  <-  "board"
  status[which(buoysOnBoard == FALSE)] <-  "water"
  status[which(is.na(buoysOnBoard))]   <- "not_classified"
  status[which(landPositions == TRUE)] <-  "land"
  status[which(stationaryLandBuoys == TRUE)] <- "land_and_stationnary"
  #status[which(portPositions == TRUE)] <-  "port"
  status[which(isolatedPositions == TRUE)] <- "isolated"
  status[which(ubiquitous == TRUE)] <- "ubiquitous"
  status[which(duplicated == TRUE)] <- "duplicated"
  
  return(status)
}


#' Title
#'
#' @param trajId 
#' @param status 
#'
#' @return
#' @export
#'
#' @examples
#' trajId2 <- set_StatusTrajId(buoy_id = data_FR$buoy_id,
#'                             trajId  = data_FR$trajId,
#'                             position_date = data_FR$position_date,
#'                             status = data_FR$buoyStatus)
#' 
set_StatusTrajId <- function(buoy_id, position_date, trajId, status)
{
  df <- data.frame(row = 1:length(buoy_id),
                   position_date = as.POSIXct(as.character(position_date)),
                   buoy_id = buoy_id, 
                   trajId = trajId, 
                   #status = ifelse(status=="water", T, F),
                   res = status)
  
  df <- plyr::ddply(.data = df, .variables = plyr::.(buoy_id), .progress ="text",
                       .fun = function(x){
                         x <- x[order(x$position_date),]
                         if(nrow(x) > 1)
                         {
                           x$compare <- c(FALSE, 
                                        x$res[1:(nrow(x)-1)] != x$res[2:(nrow(x))])
                         }else{
                           x$compare <- FALSE
                         }
                         
                         #x$trajId2 <- abs(c(0,diff(x$compare)))
                         x$trajId2 <- cumsum(x$compare)
                         #x$trajId2 <- paste0(x$res, "_", x$trajId2)
                         return(x)
                       })
  df <- df[order(df$row),]
  return(df$trajId2)
}

