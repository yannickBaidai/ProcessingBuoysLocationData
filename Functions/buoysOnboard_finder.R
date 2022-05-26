


# Title : computeMeanSpeed  ------------------------------------------------------------------------------------------------------------------
#' @param lon : numeric vector
#' @param lat : numeric vector
#' @param time : date, posixct vector
#'
#' @return
#' @export
#'
#' @examples
#' setwd("D:/RScripts/Echofad_Standard/")
#' data <-read.csv("Data/All_data/Subsample_ATL_201611.csv")
#' df <- subset(data, code == "DL+67277")
#' View(cbind.data.frame(lon =df$longitude, lat=df$latitude, time =df$date, 
#' speed =computeMeanSpeed(lon =df$longitude, lat=df$latitude, time =df$date)))
#' 
computeMeanSpeed <- function(lon, lat, time) 
{
  # cleaning time class
  time <- as.POSIXct(time, tz = 'UTC')
  if(length(time)>1)
  {
    #---Ordering time decreasingly
    idKey <- 1:length(time)
    idKey <- idKey[order(time)]  
    lon  <- lon[order(time)]
    lat  <- lat[order(time)]
    time <- time[order(time)]
    
    #---speed
    require(sp)
    dif <- diff(time, units="hours")
    units(dif) <- "hours"
    timeDiff <- as.numeric(dif)
    
    dist <- 0.5399568 *(spDists(x= data.frame(lon,lat), longlat = TRUE,   segments = TRUE))
    speedCalc <- dist/timeDiff
    speedmodulus <- rep_len(NA, length(idKey))
    speedmodulus[1] <- speedCalc[1]
    speedmodulus[2:length(idKey)] <- speedCalc
    return(speedmodulus[order(idKey)])
  }else
  {
    return(NA)
  }
}

# Title : computeLocationDataMeanSpeed ------------------------------------------------------------------------------------------------------------
#' @param buoy_id   : characrer vector of buoy_id
#' @param longitude : numeric vector
#' @param latitude  : numeric vector
#' @param time      : Date or POSIXct  vector
#' @param trajId    : optionnal sub-trajectories id
#'
#' @return  : numeric vector of speed
#' @details : apply function meanSpeed to a whole dataset
#' @examples :
#' setwd("D:/RScripts/Echofad_Standard/")
#' data <-read.csv("Data/All_data/Subsample_ATL_201611.csv")
#' speed <- computeLocationDataMeanSpeed(buoy_id = data$code, 
#'                                       longitude =data$longitude,
#'                                       latitude=data$latitude, 
#'                                       time =data$date)
computeLocationDataMeanSpeed <- function(buoy_id,  longitude, latitude, time, trajId=NULL)
{
 # base dataframe
  df <- data.frame(row = 1:length(buoy_id),
                   buoy_id = as.character(buoy_id),
                   longitude = longitude, 
                   latitude = latitude,
                   position_date = time,
                   stringsAsFactors = F)
  require(plyr) 
  cat("Computing mean speed...\n")
  if(!is.null(trajId))
  {
    df$trajId <- trajId
    meanSpeed <- ddply(.data = df, .variables = .(buoy_id, trajId), .progress ="text",
                       .fun = function(x)
                      {
                         if(is.na(unique(x$trajId)))
                         {
                           speed <- rep(NA, nrow(x))
                         }else
                         {
                           speed <- computeMeanSpeed(lon  = x$longitude,
                                                             lat  = x$latitude, 
                                                             time = x$position_date)
                         }
                         data.frame(row = x$row,
                                    speed = speed)
                      })
  }else
  {
    meanSpeed <- ddply(.data = df, .variables = .(buoy_id),  .progress ="text",
                          .fun = function(x)
                          {
                            data.frame(row = x$row,
                                       speed = computeMeanSpeed(lon  = x$longitude,
                                                                lat  = x$latitude, 
                                                                time = x$position_date))
                          })
  }
  return(meanSpeed$speed[order(meanSpeed$row)])
}


# Title : get_locationDataHistory ------------------------------------------------------------------------------------------------------------------
#' @param timePeriod : range of time window before
#' @param buoy_id    : character value of buoy_id for which speed historic is required
#' @param timestamp  : date or POSIXct value for which speed historic is required
#' @param speed  : numeric vector of speed value 
#' @param trajId    : ( vector) optionnal sub-trajectories id
#' @param useParallel  : use parallel computations
#'
#' @return  : numeric vector of max speed over a period of three days before each timestamp
#'
#' @examples :
#' setwd("D:/RScripts/Echofad_Standard/")
#' data <-read.csv("Data/All_data/Subsample_ATL_201611.csv")
#' data$speed <- computeLocationDataMeanSpeed(buoy_id = data$code, 
#'                                       longitude =data$longitude,
#'                                       latitude=data$latitude, 
#'                                       time =data$date)
#' data$speed_history <-  get_locationDataHistory(buoy_id = data$code,
#'                                     timestamp = data$date,
#'                                     speed = data$speed,
#'                                     trajId = NULL, 
#'                                     timePeriod = 3, 
#'                                     useParallel = T)
get_locationDataHistory <- function(buoy_id,
                                    timestamp,
                                    speed,
                                    trajId = NULL, 
                                    timePeriod = 3, 
                                    useParallel)
{
  # formatting timestamp
  timestamp <- as.POSIXct(as.character(timestamp), tz="UTC")
  
  # base dataframe
  data <- data.frame(row = 1:length(buoy_id),
                     buoy_id = buoy_id,
                     timestamp = timestamp,
                     speed = speed)
  require(plyr)
  require(lubridate)
  #---Computations blocks (with parallelisation)
  if(useParallel == T){
    cat("\t\t(Verbose disable while using parallel !)\n")
    library(doParallel)
    # Defining numbers of clusters to create
    coresNb <- ifelse(detectCores()> 2, detectCores() -2, 2)
    clus <- makeCluster(coresNb)
    registerDoParallel(clus)
  }
  
  # Getting speed history
  if(is.null(trajId))
  {
    data <- ddply(.data = data, .variables = .(buoy_id), .progress = "text", .parallel = useParallel,
                  .fun = function(df)
                  {
                    # get mas speed over 3 days for each row of df
                      speedHistory <- adply(.data = df, .margins = 1,
                                            .fun = function(x)
                                            {
                                              # Get for each entry the history of speed i days before
                                              current_timeStamp <- x$timestamp
                                              limit_timeStamp   <- as.Date(current_timeStamp - lubridate::days(timePeriod+1))
                                              current_speedHistory <- df[which(df$timestamp <  current_timeStamp &
                                                                               as.Date(df$timestamp) >= limit_timeStamp), ]	
                                              # Check results
                                              if(length(current_speedHistory)==0)
                                              {
                                                return(data.frame(historical_max_speed= NA))
                                              }else
                                              {
                                                #--Check if current history is really for i days
                                                loc <- as.Date(current_speedHistory$timestamp)
                                                historyLength <- length(loc[which(loc != as.Date(current_timeStamp))])
                                                
                                                if(historyLength >= timePeriod)
                                                {
                                                    maxSpeed <- max(current_speedHistory$speed)
                                                    return(data.frame(historical_max_speed= maxSpeed))
                                                }else
                                                {
                                                  return(data.frame(historical_max_speed= NA))
                                                }
                                              }
                                            })
                      return(speedHistory)
                  })
  }else{
    data <- ddply(.data = data, .variables = .(buoy_id, trajId), .progress = "text", .parallel = useParallel,
                  .fun = function(df)
                  {
                    # get mas speed over 3 days for each row of df
                    speedHistory <- adply(.data = df, .margins = 1,
                                          .fun = function(x)
                                          {
                                            # Get for each entry the history of speed i days before
                                            current_timeStamp <- x$timestamp
                                            limit_timeStamp   <- as.Date(current_timeStamp - lubridate::days(timePeriod+1))
                                            current_speedHistory <- df[which(df$timestamp <  current_timeStamp &
                                                                               as.Date(df$timestamp) >= limit_timeStamp), ]	
                                            # Check results
                                            if(length(current_speedHistory)==0)
                                            {
                                              return(data.frame(historical_max_speed= NA))
                                            }else
                                            {
                                              #--Check if current history is really for i days
                                              loc <- as.Date(current_speedHistory$timestamp)
                                              historyLength <- length(loc[which(loc != as.Date(current_timeStamp))])
                                              
                                              if(historyLength >= timePeriod)
                                              {
                                                maxSpeed <- max(current_speedHistory$speed)
                                                return(data.frame(historical_max_speed= maxSpeed))
                                              }else
                                              {
                                                return(data.frame(historical_max_speed= NA))
                                              }
                                            }
                                          })
                    return(speedHistory)
                  })
  }
  
  # Stop cluster
  if(useParallel == T){
    stopCluster(clus)
  }
  
  # return historic for the whole dataset
  return(data$historical_max_speed[order(data$row)])
}


# Title : accelIndex_calc ------------------------------------------------------------------------------------------------------------------
# 
#' Title
#' @param buoy_id    : character value of buoy_id for which speed historic is required
#' @param timestamp  : date or POSIXct value for which speed historic is required
#' @param speed  : numeric vector of speed value 
#' @param trajId    : ( vector) optionnal sub-trajectories id
#' @param useParallel  : use parallel computations
#'
#' @return
#' @details : Computes consecutive speeds variation
#'
#' @examples
#' setwd("D:/RScripts/Echofad_Standard/")
#' data <-read.csv("Data/All_data/Subsample_ATL_201611.csv")
#' data$speed <- computeLocationDataMeanSpeed(buoy_id = data$code, 
#'                                       longitude =data$longitude,
#'                                       latitude=data$latitude, 
#'                                       time =data$date)
#' accel_values <- accelIndex_calc   (buoy_id = data$code,
#'                                     timestamp = data$date,
#'                                     speed = data$speed,
#'                                     trajId = NULL, 
#'                                     useParallel = F)  
#' data <- cbind.data.frame(data, accel_values)                               
accelIndex_calc <- function(buoy_id,
                            timestamp,
                            speed,
                            trajId = NULL,
                            useParallel = F)
{
  # formatting timestamp
  timestamp <- as.POSIXct(as.character(timestamp), tz="UTC")
  
  # base dataframe
  data <- data.frame(row = 1:length(buoy_id),
                     buoy_id = buoy_id,
                     timestamp = timestamp,
                     speed = speed)
  require(plyr)
  require(lubridate)
  # Computations blocks (with parallelisation)
  if(useParallel == T){
    cat("\t\t(Verbose disable while using parallel !)\n")
    library(doParallel)
    # Defining numbers of clusters to create
    coresNb <- ifelse(detectCores()> 2, detectCores() -2, 2)
    clus <- makeCluster(coresNb)
    registerDoParallel(clus)
  }
  
  # Accel index computations
  if(is.null(trajId))
  {
    data <- ddply(.data = data, .variables = .(buoy_id), .progress = "text", .parallel = useParallel,
                  .fun = function(df)
                  {
                    #1. Compute speed variation from start to end
                    df <- df[order(df$timestamp, decreasing = FALSE),]
                    df$accel <- rep(NA, nrow(df))
                    if(nrow(df) > 1)
                    {
                      diffspeed <- abs(diff(df$speed, lag=1))
                      df$accel[2: nrow(df)] <- diffspeed
                    }
                    
                    #2. Compute Speed Variation from end to start
                    df <- df[order(df$timestamp, decreasing = TRUE),]
                    df$accel_inv <- rep(NA, nrow(df))
                    if(nrow(df) > 1)
                    {
                      diffspeed_inv <- abs(diff(df$speed, lag=1))
                      df$accel_inv [2: nrow(df)] <- diffspeed_inv
                    }
                    return(df)
                  })
  }else
  {
    data <- ddply(.data = data, .variables = .(buoy_id, trajId), .progress = "text", .parallel = useParallel,
                  .fun = function(df)
                  {
                    #1. Compute speed variation from start to end
                    df <- df[order(df$timestamp, decreasing = FALSE),]
                    df$accel <- rep(NA, nrow(df))
                    if(nrow(df) > 1)
                    {
                      diffspeed <- abs(diff(df$speed, lag=1))
                      df$accel[2: nrow(df)] <- diffspeed
                    }
                    
                    #2. Compute Speed Variation from end to start
                    df <- df[order(df$timestamp, decreasing = TRUE),]
                    df$accel_inv <- rep(NA, nrow(df))
                    if(nrow(df) > 1)
                    {
                      diffspeed_inv <- abs(diff(df$speed, lag=1))
                      df$accel_inv [2: nrow(df)] <- diffspeed_inv
                    }
                    return(df)
                  })
  }
  
  # Stop cluster 
  if(useParallel == T){
    stopCluster(clus)
  }
  
  # Ordering and and returning data
  return(data[order(data$row), c("accel", "accel_inv")])
}

# Title : get_pureSequence ------------------------------------------------------------------------------------------------------------------


get_pureSequence <- function(sequence)
{
  #--------------------------------------------------------------------------------------------------------
  #--recup. des sequences pures de bouees en mer (aucun changement d'etat)
  #--------------------------------------------------------------------------------------------------------
  sea_seq <- which(sequence == TRUE)
  if(length(sea_seq) != 0)
  {
    res <- rep_len(NA, length(sea_seq))
    sea <- TRUE
    board <- FALSE
    for(i in 1:length(sea_seq))
    {
      j <- sea_seq[i]
      prevPt <- ifelse(j==1, NA, sequence[j-1])
      nextPt  <- ifelse(j==length(sequence), NA, sequence[j+1])
      
      if(!is.na(prevPt) && !is.na(nextPt))
      {
        if(prevPt== sea && nextPt == sea){
          res[i] <- j
        }else{
          res[i] <- NA
        }
      }else
      {
        res[i] <- NA
      }
    }
    sea_seq <- na.omit(res)
    rm(res)
  }
  
  
  #--------------------------------------------------------------------------------------------------------
  #--recup. des sequences pures de bouÃ©es a bord du bateau (aucun changement d'etat)
  #-------------------------------------------------------------------------------------------
  board_seq <- which(sequence == FALSE)
  if(length(board_seq) != 0)
  {
    sea <- TRUE
    board <- FALSE
    
    res <- rep_len(NA, length(board_seq))
    for(i in 1:length(board_seq))
    {
      j <- board_seq[i]
      prevPt <- ifelse(j==1, NA, sequence[j-1])
      nextPt  <- ifelse(j==length(sequence), NA, sequence[j+1])
      
      if(!is.na(prevPt) && !is.na(nextPt))
      {
        if(prevPt==board && nextPt == board){
          res[i] <- j
        }else{
          res[i] <- NA
        }
      }else
      {
        res[i] <- NA
      }
    }
    board_seq <- na.omit(res)
    rm(res)
  }
  
  
  #--------------------------------------------------------------------------------------------------------
  #--recup. des transitions bouÃ©es -bateaux
  #--------------------------------------------------------------------------------------------------------
  res <- rep_len(NA, length(sequence))
  sea <- TRUE
  board <- FALSE	
  for(i in 1:length(sequence))
  {
    prevPt <- ifelse(i==1, NA, sequence[i-1])
    nextPt  <- ifelse(i==length(sequence), NA, sequence[i+1])
    currPt <- sequence[i]
    
    if(!is.na(prevPt) && !is.na(currPt))
    {
      if(prevPt == currPt){
        res[i] <- NA
      }else{
        res[i] <- i
      }
    }else{
      res[i] <- NA
    }
  }
  change_seq  <- na.omit(res)
  rm(res)
  
  ls <-list(seaSeq = sea_seq, 
            boardSeq = board_seq, 
            changeSeq = change_seq)
  
  return(ls)
}

get_stateParameters <- function(sequences, plot =  TRUE, returnTab = FALSE)
{
  library(foreach)
  # Sea seq
  seaseq_accel <- foreach(i = 1:length(sequences), .combine = c)%do%
  {
    unlist(sequences[[i]]$sea, recursive = FALSE, use.names = FALSE)
  }

  # Board sed
  boardSeq_accel <- foreach(i = 1:length(sequences), .combine = c)%do%
  {
    unlist(sequences[[i]]$board, recursive = FALSE, use.names = FALSE)
  }

  # Transition seq
  changeSeq_accel <- foreach(i = 1:length(sequences), .combine = c)%do%
  {
    unlist(sequences[[i]]$transition, recursive = FALSE, use.names = FALSE)
  }

  
  # seaseq_accel <- unlist(seaseq_accel)
  # boardSeq_accel <- unlist(boardSeq_accel)
  # changeSeq_accel <- unlist(changeSeq_accel)
  
  if(length(seaseq_accel)>0)
  {
    seaDf <- data.frame(group = rep("sea", length(seaseq_accel)), accel_index =  seaseq_accel)
  }else{
    seaDf <- data.frame(group = ("sea"), accel_index =  NA)
  }
  
  if(length(boardSeq_accel)>0)
  {
    boardDf <- data.frame(group = rep("board", length(boardSeq_accel)), accel_index =  boardSeq_accel)
  }else{
    boardDf <- data.frame(group = ("board"), accel_index =  NA)
  }
  
  if(length(changeSeq_accel)>0)
  {
    changeDf <- data.frame(group = rep("transition", length(changeSeq_accel)), accel_index =  changeSeq_accel)
  }else{
    changeDf <- data.frame(group = ("transition"), accel_index =  NA)
  }
  
  df <- rbind.data.frame(seaDf, boardDf, changeDf)
  df$group <- as.factor(df$group)
  if(plot==TRUE)
  {
    dev.new()
    boxplot(accel_index~group, data = df, main = "Distribution of speed variation ")
  }
  
  if(returnTab==TRUE)
  {
    return(df)
  }
  #compute IC from t-test
  sea_IC <- tryCatch(t.test((seaseq_accel))$conf.int, error = function(err) {return (c(NA, NA))}, silent=TRUE)
  board_IC <- tryCatch(t.test((boardSeq_accel))$conf.int, error = function(err) {return (c(NA, NA))}, silent=TRUE)
  change_IC <- tryCatch(t.test((changeSeq_accel))$conf.int, error = function(err) {return (c(NA, NA))}, silent=TRUE)

  seaSeq_par <- data.frame(inf = sea_IC[1], mean = mean(seaseq_accel, na.rm = TRUE), sup = sea_IC[2])
  boardSeq_par <- data.frame(inf = board_IC[1],  mean = mean(boardSeq_accel, na.rm = TRUE), sup = board_IC[2])
  changeSeq_par <- data.frame(inf = change_IC[1],  mean =  mean(changeSeq_accel, na.rm = TRUE), sup = change_IC[2])

  # from boxplot
  # sea_IC <- tryCatch(boxplot(seaseq_accel, plot =F)$stats, error = function(err) {return (c(NA, NA))}, silent=TRUE)
  # board_IC <- tryCatch(boxplot(boardSeq_accel, plot =F)$stats, error = function(err) {return (c(NA, NA))}, silent=TRUE)
  # change_IC <- tryCatch(boxplot(changeSeq_accel, plot =F)$stats, error = function(err) {return (c(NA, NA))}, silent=TRUE)
  # 
  # seaSeq_par <- data.frame(inf = sea_IC[1], mean = mean(seaseq_accel, na.rm = TRUE), sup = sea_IC[5])
  # boardSeq_par <- data.frame(inf = board_IC[1],  mean = mean(boardSeq_accel, na.rm = TRUE), sup = board_IC[5])
  # changeSeq_par <- data.frame(inf = change_IC[1],  mean =  mean(changeSeq_accel, na.rm = TRUE), sup = change_IC[5])
  
  summaryTab <- rbind(seaSeq_par,boardSeq_par, changeSeq_par)
  rownames(summaryTab) <-  c("sea", "ship", "transition")
  return(summaryTab)
}

# compare_accelIndex <- function(stateParameters, accel) # Choose the right comparison according to state parameters data (case some parameters = NA)
# {  
#   # check if inf at sea states
#   if(!is.na(stateParameters["ship", "sup"])){
#     accelSup <- stateParameters["ship", "sup"]
#     expr <- (as.numeric(accel) <= accelSup)
#   }else{
#     if(!is.na(stateParameters["transition","inf"])){
#       accelInf <- stateParameters["transition","inf"]
#       expr <- (as.numeric(accel) < accelInf)
#     }else{
#       if(!is.na(stateParameters["ship", "mean"])){
#       accelSup <- stateParameters["ship", "mean"]
#       expr <- (as.numeric(accel) <= accelSup)
#       }else{
#         if(!is.na(stateParameters["transition","mean"])){
#           accelInf <- stateParameters["transition","mean"]
#           expr <- (as.numeric(accel) < accelInf)
#         }else{
#           if(!is.na(stateParameters["sea", "sup"])){
#             accelSup <- stateParameters["sea","sup"]
#             #expr <- ifelse(as.numeric(accel) <= accelSup, TRUE, NA)
#             expr <- (as.numeric(accel) <= accelSup)
#           }else{
#             if(!is.na(stateParameters["sea", "mean"])){
#               accelSup <- stateParameters["sea","mean"]
#               #expr <- ifelse(as.numeric(accel) <= accelSup, TRUE, NA)
#               expr <- (as.numeric(accel) <= accelSup)
#             }else{
#               expr <- NA
#             }
#           }
#         }
#       }
#     }
#   }
#   return(expr)
# }

compare_fromSeaStates <- function(stateParameters, accel)
{
  if(is.na(stateParameters["sea", "sup"]))
  {return(NA)}
  accelSup <- stateParameters["sea", "sup"]
  return(ifelse(as.numeric(accel) <= accelSup, TRUE, NA))
  
}

compare_fromBoardStates <- function(stateParameters, accel)
{
  if(!is.na(stateParameters["ship", "inf"]))
  {return(NA)}
  accelInf <- stateParameters["ship", "inf"]
  return(as.numeric(accel) <= accelInf)
}

compare_fromTransitionStates <- function(stateParameters, accel)
{
  if(is.na(stateParameters["transition","inf"]))
  {return(NA)}
  accelInf <- stateParameters["transition","inf"]
  return(as.numeric(accel) < accelInf)
}

compare_accelIndex <- function(stateParameters, accel)
{
  expr <- compare_fromSeaStates(stateParameters, accel)
  if(is.na(expr))
  {
    expr <- compare_fromTransitionStates(stateParameters, accel)
    if(is.na(expr))
    {
      expr <- compare_fromBoardStates(stateParameters, accel)
    }
  }
  return(expr)
}

defineState_from <- function(accel, othPt, tab = stateParameters)
{
  state <- NA
  
  if(is.na(accel) || is.na(othPt) || is.na(compare_accelIndex(tab, accel)))
  {
    warning(paste0("State definition not possible ! \n", 
                   "\t - acceleration index = ", accel, "\n",
                   "\t - closest point state   = ", othPt, "\n",
                   "\t - Accels comparison return : ", compare_accelIndex(tab, accel), "\n"))
  }else
  { #---accel index close to 0 : constant state with the previous tPt
    if(compare_accelIndex(tab, accel))
    {
      state <- othPt
    }else#transitional state
    {
      state <- !othPt
    }
  }
  return(state)
}

stateAssignment_correction <- function(id_key, state, accel, accel_inv, transitionalAccelVar)
{
  if(length(state) != length(accel)){stop("Length of paremeters state and accel differ !")}
  
  #---NA state assignment
  naState  <- which(is.na(state))
  if(length(naState) >0)
  {
    prevPt<- NA
    curPt <- NA
    nextPt<- NA
    nextPtaccel <- NA
    curPtaccel  <- NA
    
    #--First direction
    for(i in 1:length(naState))
    {
      j <- naState[i]
      prevPt      <- ifelse(j == 1, NA, state[j-1])    
      nextPt      <- ifelse(j == length(state), NA, state[j+1])
      nextPtaccel <- ifelse(j == length(state), NA, accel[j+1])
      
      curPt       <- state[j]
      curPtaccel  <- accel[j]
      
      #try defining state from prevPt and cur accel (or nextpt and nextACcel)
      state[j] <- tryCatch(expr =
                                {
                                  cat("\t - 1st direction : 1st attempt...")
                                 value <- defineState_from(accel = curPtaccel, othPt = prevPt, tab =transitionalAccelVar)
                                  if(is.na(value)){
                                    cat("failed.\n")
                                  }else{
                                    cat("succeed\n")
                                  }
                                  value
                                },
        
                           warning = function (e){
                             cat("\t - Second attempt...")
                             value <- (defineState_from(accel = nextPtaccel,  othPt = nextPt, tab =transitionalAccelVar))
                             if(is.na(value)){
                               cat("failed.\n")
                             }else{
                               cat("succeed\n")
                             }
                             return(value)
                           },
                           silent=TRUE)
    }
    
    #--inverse direction for all trajectory
    for(j in length(state):1)
    {
      prevPt      <- ifelse(j == length(state), NA, state[j+1])    
      nextPt      <- ifelse(j == 1, NA, state[j-1])
      nextPtaccel <- ifelse(j == 1, NA, accel_inv[j-1])
      
      curPt       <- state[j]
      curPtaccel  <- accel_inv[j]
      
      #try defining Na state from prevPt and cur accel (or nextpt and nextACcel)
      if(is.na(state[j] ))
      {
        state[j] <- tryCatch(expr = 
                               {
                                 cat("\t - 2nd direction : 1st attempt...")
                                 value <- defineState_from(accel = curPtaccel, othPt = prevPt, tab =transitionalAccelVar)
                                 if(is.na(value)){
                                   cat("failed.\n")
                                 }else{
                                   cat("succeed\n")
                                 }
                                 value
                                },
                             warning = function (e){
                               cat("\t - Second attempt...")
                               value <- defineState_from(accel = nextPtaccel,  othPt = nextPt, tab =transitionalAccelVar)
                               if(is.na(value)){
                                 cat("failed.\n")
                               }else{
                                 cat("succeed\n")
                               }
                               return(value)
                             },
                             silent=TRUE)
      }
    }
  }
  return(data.frame(topiaid = id_key, is_atSea = state))
}

correct_SingleTransitionnalState <- function (id_key, timestamp,  state, transitionDurationThreshold = 24, forceAssignation = TRUE, returnAll = FALSE)
{
  library(plyr)
  library(lubridate)

  if(length(state) > 1)# au moins deux positions sur la trajectoire
  {
    transState <- rep(NA, length(state))
    transState[2:length(state)] <- diff(state, lag =1)
    
    df <- data.frame(topiaid = id_key,
                     timeStamp= timestamp,
                     is_atSea = state,
                     is_transState = transState)
    rm(id_key, state, timestamp, transState)
    
    state_ptr <- which(abs(df$is_transState) == 1)
    
    if(length(state_ptr) > 0)
    {
      for(i in 1:length(state_ptr))
      {
        j <- state_ptr[i]
        
        if(!is.na(df$is_transState[j]) && df$is_transState[j] != 0)#--Ne pas modifer les etats deja corriger (transtate = 0)
        {
          currState <- df$is_atSea[j]
          prevState 	 <- ifelse(j ==1, NA, df$is_atSea[j-1])
          next24h_ptr  <- which(df$timeStamp >= (df$timeStamp[j] + hours(transitionDurationThreshold)))
          if(length(next24h_ptr)>0)
          {
            ptr <- min(next24h_ptr)
            next24hState <- df$is_atSea[ptr]
            #--Corrections des etats transitionnels inercal?s
            if(!is.na(next24hState) && prevState ==  next24hState)
            {
              if(forceAssignation == FALSE){
                df$is_atSea[j:(ptr-1)] <- NA
              }else{
                df$is_atSea[j:(ptr-1)] <- prevState
              }
              df$is_transState[2:nrow(df)] <- diff(df$is_atSea, lag =1)
            }
          }
        }
      }
    }
  }else{
    df <- data.frame(topiaid = NA,
                     timeStamp= NA,
                     is_atSea = NA,
                     is_transState = NA)
  }
  
  if(returnAll == TRUE)
  {
    return(df)
  }
  else
  {
    return(df[, c("topiaid", "is_atSea")])
  }
}


#' Title
#'
#' @param longitude 
#' @param latitude 
#' @param buffer 
#' @param appDir 
#' @param plot 
#'
#' @return
#' @export
#'
#' @examples
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


#' Title
#'
#' @param buoy_id 
#' @param longitude 
#' @param latitude 
#' @param timestamp 
#' @param meanSpeed 
#' @param buoysOnBoard 
#' @param buffer 
#' @param delta_v_threshold 
#' @param speed_threshold 
#' @param time_threshold 
#' @param verbose 
#' @param ... 
#'
#' @return
#' @export
#'
#' @examples
#' 
#'  df <- subset(data, code == "M3I196678")
sri_lanka_correction <- function(id, buoy_id,longitude,latitude, timestamp, meanSpeed, buoysOnBoard,
                                 buffer = 1, # about 20 km around Sri lanka island
                                 delta_v_threshold = 2, speed_threshold = 3, time_threshold = 48, 
                                 verbose = T)#,
                                 #...)
{
  
  df <- data.frame(topiaid = id,
                   buoy_id = as.character(buoy_id),
                   longitude = longitude,
                   latitude = latitude,
                   position_date = timestamp,
                   meanSpeed = meanSpeed,
                   is_onBoardShip = buoysOnBoard,
                   stringsAsFactors = F) 
  
  #---1. Identifying buoys in sri lanka area
  df$is_closeToSriLanka <- is_it_close_to_sri_lanka(df$longitude, df$latitude,
                                                    buffer = buffer,  plot = F)
													
  #df$is_closeToSriLanka <- is_it_close_to_sri_lanka(df$longitude, df$latitude,
  #                                                 buffer = buffer,  plot = F, ...)
  
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
                     
                     #---checking if a outlier have been detected
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
                         cat("\n+ Correcting assignment for buoy : ", unique(as.character(x$buoy_id)), "- Sea period :", unique(x$seaPeriod_bfr_sriLanka),"\n")
                         cat("\t- from : ", as.character(x$position_date[startCorrectionFrom]),"\n")
                         cat("\t- to   : ", as.character(x$position_date[endCorrectionAt]),"\n")
                         cat("\t- delta_v outliers :", paste(round(outlier, 2), collapse = "; "), "\n")
                       }
                       x$is_onBoardShip[startCorrectionFrom:endCorrectionAt] <- TRUE
                       
                     }
                     #---checking if a high spped buoys have been detected
                     if(length(highSpeedBuoys)>1)
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

#------------------------------------------------------------------------------------------------------------------
#' Title
#'
#' @param buoy_id    : character value of buoy_id for which speed historic is required
#' @param trajId    : ( vector) optionnal sub-trajectories id
#' @param longitude 
#' @param latitude 
#' @param TIME_PERIOD_HISTORY : integer value of time window of speed history
#' @param MAX_DRIFTING_SPEED ; integer value of theoretical  drifting speed limit
#' @param transitionDuration : 
#' @param useParallel : use parallel computations
#' @param timestamp 
#' @param forcedAssignment 
#' @param showMap 
#' @param showStat 
#'
#' @return : dataframe 
#' @export
#'
#' @examples
#' setwd("D:/RScripts/Echofad_Standard/")
#' data <-read.csv("Data/All_data/Subsample_ATL_201611.csv")
#' source("Functions/isolatedPositions_finder.R")
#' data <- cbind.data.frame(data, 
#'                         isolatedPositions_finder(buoyId =data$code, positionDate = data$date, longitude = data$longitude, latitude = data$latitude,  showMap = T, showStat = T, single.threshold = 1))
#' df <- buoysOnBoard_finder(buoy_id = data$code, trajId = data$trajId,
#'                           longitude =data$longitude,
#'                           latitude=data$latitude, 
#'                           timestamp =data$position_date)
buoysOnBoard_finder <- function(buoy_id, trajId,
                                longitude, latitude,
                                timestamp, 
                                isolatedPositions,
                                TIME_PERIOD_HISTORY = 3,
                                MAX_DRIFTING_SPEED = 6, 
                                transitionDuration = 24, 
                                forcedAssignment = TRUE, useParallel = F,
                                showMap = T, showStat = T)
{
  cat("Buoys on Board Finder :\n")
  # Base.dataframe
  df <- data.frame(row = 1:length(buoy_id), buoy_id = buoy_id, trajId = trajId,
                   longitude = longitude, latitude = latitude,
                   position_date = as.POSIXct(as.character(timestamp), tz="UTC"),
                   isolatedPositions = isolatedPositions)
  
  
  
  # 1. Compute meanspeed  ------------------------------
  cat("\t + Computing mean speed between consecutive positions...\n")
  df$meanSpeed <- computeLocationDataMeanSpeed(buoy_id = df$buoy_id, trajId = df$trajId,
                                               longitude = df$longitude, latitude = df$latitude,
                                               time = df$position_date)
  
  # 2. Removing duplicated location_topiaid due to errors between assigation of gps data to echodata in raw database  ------------------------------
  df_subset <- df[!(duplicated(df[,c("buoy_id", "position_date")], fromLast = TRUE)),]
  df_subset <- df_subset[order(df_subset$position_date), ]
  
  # filtering data
  df_subset <- subset(df_subset, isolatedPositions == F &
                                 !is.na(trajId))
  
  # 3. Assignment I ; Assigning buoys state from  buoys speed (MAX_DRIFTING_SPEED : board buoys)  ------------------------------
  df_subset$is_atSea <- rep(NA, nrow(df_subset))
  df_subset$is_atSea[which(df_subset$meanSpeed >= MAX_DRIFTING_SPEED)] <- FALSE
  
  # 4. Assignment I ; Computing speed history  ------------------------------
  cat("\t + Primary assignment of buoys status through data history...\n")
  cat(paste0("\t\t- Temporal  range set to     : ", TIME_PERIOD_HISTORY, " days.\n"))
  cat(paste0("\t\t-  Max drifting speed set to : ", MAX_DRIFTING_SPEED, " knots.\n"))
  df_subset$historical_max_speed <- get_locationDataHistory(buoy_id     = df_subset$buoy_id,
                                                            timestamp   = df_subset$position_date,
                                                            speed       = df_subset$meanSpeed,
                                                            trajId      = df_subset$trajId, 
                                                            timePeriod  = TIME_PERIOD_HISTORY, 
                                                            useParallel = useParallel)
  
  # 5. Assignment I ; Assigning  buoys on board from history of max speed drifting (MAX_DRIFTING_SPEED : buoys at sea)  ------------------------------
  df_subset$is_atSea[which(df_subset$historical_max_speed < MAX_DRIFTING_SPEED)] <- TRUE
  
  
  # 6. Assignment II : Computes consecutive speeds variation ------------------------------
  accelValues <- accelIndex_calc(buoy_id = df_subset$buoy_id,
                                 timestamp = df_subset$position_date,
                                 speed = df_subset$meanSpeed,
                                 trajId = df_subset$trajId,
                                 useParallel = useParallel)
  df_subset <- cbind.data.frame(df_subset, accelValues)
  rm(accelValues)
  invisible(gc())
  
  # 7. Get transitionnal and constants states parameters -----------------------------------
  cat("\t + Computing transitionnal and constants states parameters...\n")
  sequences <- dlply(.data = df_subset, .variables = .(buoy_id, trajId), function (x)
  {
    seq <- get_pureSequence(x$is_atSea)
    seaseq_accel    <- x$accel[unlist(seq$seaSeq, recursive = FALSE, use.names = FALSE)]
    boardSeq_accel  <- x$accel[unlist(seq$boardSeq, recursive = FALSE, use.names = FALSE)]
    changeSeq_accel <- x$accel[unlist(seq$changeSeq, recursive = FALSE, use.names = FALSE)]
    return(list(sea = seaseq_accel,
                board = boardSeq_accel,
                transition = changeSeq_accel))
  },
  .progress = "text",
  .parallel = FALSE)
  
  save(sequences, file = file.path(getwd(),"sequences_data.RData"))
  message("sequences data saved to :", file.path(getwd(),"sequences_data"))
  stateParameters <- get_stateParameters(sequences, plot=TRUE)
  
  rm(sequences)
  invisible(gc())
  
  
  # 8. Acceleration computations and Correction of buoys state assignment according to  acceleration (transitional acceleration analyses index from prior analysis) --------------------------------
  cat("\t + Secondary buoy status assignment through speed variation...\n")
  buoysStatus <- ddply(.data = df_subset, .variables = .(buoy_id, trajId),   .progress = "text", .parallel = FALSE,
                       .fun = function(zz)
                         {
                            stateAssignment_correction(id_key = zz$row,
                                                       state = zz$is_atSea,
                                                       accel = zz$accel,
                                                       accel_inv = zz$accel_inv,
                                                       transitionalAccelVar = stateParameters)
                          })
  
  df_subset$is_atSea <- buoysStatus$is_atSea[match(df_subset$row, buoysStatus$topiaid)]  
  
  # 9. Correction of intermediates transitionnal states with duration lower than a threshold (default = 24h)--------------------------
  cat("\t + Final buoy status assignment : correction of single transitionnal state (due to buoys on a drifting ship)...\n")
  buoysStatus <- ddply(.data = df_subset, .variables = .(buoy_id, trajId),.progress = "text",.parallel = FALSE,
                       .fun = function(yy)
                         {
                              correct_SingleTransitionnalState(id_key = yy$row,
                                                               timestamp = yy$position_date,
                                                               state = yy$is_atSea,
                                                               transitionDurationThreshold= transitionDuration,
                                                               forceAssignation = forcedAssignment,
                                                               returnAll = T)
                            })
  
  
  # 10. Final data assignment into echodata tables ---------------------------------------------------
  df$buoysOnBoard <- ! buoysStatus$is_atSea[match(df$row, buoysStatus$topiaid)]
  
  #-----Update (bug reslution - Yannick BAIDI-28/09/2017 - 12:37) :  ---------------------------------------
  #        The precedent step (correction ntermediates transitionnal states with duration lower than a threshold)
  #        leads to reattribution of primary board assignment (according to max drifting speed), we  correct the
  #        bug here by reassigning those data to their original state
  #---Assign buoys on board from history of max speed drifting (MAX_DRIFTING_SPEED)
  df$buoysOnBoard[which(df$meanSpeed >= MAX_DRIFTING_SPEED)] <- TRUE
  
  
  # SRI-LANKA correction
  # 11. SriLanka correction ot buoys on board -----------------------------------------------------
  cat("\t + SRI LANKA correction...\n")
  df_sri  <- plyr::ddply(.data= df, .variables= .(buoy_id), .progress = "text",
                                              .fun = function(tb)
                                              {
                                                res <- sri_lanka_correction(id = tb$row, buffer = 3,
                                                                     buoy_id = tb$buoy_id,
                                                                     longitude = tb$longitude,
                                                                     latitude = tb$latitude, 
                                                                     timestamp = tb$position_date, 
                                                                     meanSpeed = tb$meanSpeed,
                                                                     buoysOnBoard = tb$buoysOnBoard)
                                                return(data.frame(row = res$topiaid, buoysOnBoard = res$is_onBoardShip))
                                              })
  
  df$buoysOnBoard <- df_sri$buoysOnBoard[match(df$row, df_sri$row)]
  
  # Showing optionnal Maps ---------------------------------------------
  if(showMap == TRUE)
  {
    require(ggplot2)
    world <- map_data("world")
    coords <- subset(df, isolatedPositions == F &
                         !is.na(trajId))
    coords <- coords[order(coords$position_date),]
    coords <- ddply(.data = coords, .variables = .(buoy_id, trajId), 
                   .fun = function(x){
                     x$trajId2 <- abs(c(0,diff(x$buoysOnBoard)))
                     x$trajId2 <- cumsum(x$trajId2)
                      return(x)
                   })

    p <- ggplot(data = coords, show.legend = F) +
    # World map plotting
    geom_map(data=world,
             map=world,
             aes(map_id=region),
             color="white", fill="grey", size=0.05, alpha=1)+
    # Overplotting points
    geom_point(mapping = aes(x=longitude, y=latitude, group = paste(buoy_id, trajId2), color = buoysOnBoard),
               size = 0.8,show.legend = T)+ 
    geom_path(mapping = aes(x=longitude, y=latitude, group = paste(buoy_id, trajId2)), linetype = 3, color = "black")+ 
    # Zoom
    coord_cartesian(xlim = c(min(coords$longitude), max(coords$longitude)),
                    ylim = c(min(coords$latitude), max(coords$latitude)))+
    # Labelling
    xlab("longitude")+
    ylab("latitude")+
    labs(title = "Buoys tracks",
         fill = "Density")+
    theme_minimal()+
    theme(panel.background = element_rect(fill = "white", 
                                          colour = "gray50",
                                          size = 0.5, linetype = "solid"),
          panel.grid.major = element_line(size = 0.5, linetype = 'dashed',
                                          colour = "white"), 
          panel.grid.minor = element_line(size = 0.25, linetype = 'dashed',
                                          colour = "white"))+
      facet_wrap(~buoysOnBoard)
    dev.new(); plot(p)
  }
  
  # Showing stats -------------------------------------------------------------
  if(showStat)
  {
    N <- nrow(df)
    tab <- as.data.frame(prop.table(table(df$buoysOnBoard, useNA = "always")))
    names(tab)[1] <- c("Buoys on boardship")
    message("- \t ", N, " buoy observation processed :\n")
    print(tab)
  }
  return (df$buoysOnBoard[order(df$row)])
}


