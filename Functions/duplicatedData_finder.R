#' Title
#' @author : Yannick BAIDAI
#' @date  : 29-08-2018
#' @place : Abidjan
#'
#' @param buoyId
#' @param positionDate
#' @param latitude
#' @param longitude
#' @param showStat
#'
#' @return
#' @export
#'
#' @examples
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

