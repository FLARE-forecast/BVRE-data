qaqc_bvr <- function(
            data_file = 'https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data/bvre-waterquality.csv',
            data2_file = 'https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/current_files/BVRplatform_L1.csv',
            maintenance_file = './Data/DataAlreadyUploadedToEDI/EDIProductionFiles/MakeEML_BVRplatform/BVR_maintenance_log.csv',  
            output_file, 
            start_date = NULL, 
            end_date = NULL)
{
  
  # Call the source function to get the depths
  #source("./Data/DataAlreadyUploadedToEDI/EDIProductionFiles/MakeEML_BVRplatform/2023/find_depths.R")
  source_url("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast/main/targets/target_functions/find_depths.R")
  
  #change column names
  BVRDATA_COL_NAMES = c("DateTime", "RECORD", "CR6Battery_V", "CR6Panel_Temp_C", "ThermistorTemp_C_1",
                        "ThermistorTemp_C_2", "ThermistorTemp_C_3", "ThermistorTemp_C_4", "ThermistorTemp_C_5",
                        "ThermistorTemp_C_6", "ThermistorTemp_C_7", "ThermistorTemp_C_8", "ThermistorTemp_C_9",
                        "ThermistorTemp_C_10","ThermistorTemp_C_11","ThermistorTemp_C_12","ThermistorTemp_C_13",
                        "RDO_mgL_6", "RDOsat_percent_6", "RDOTemp_C_6", "RDO_mgL_13",
                        "RDOsat_percent_13", "RDOTemp_C_13", "EXO_Date", "EXO_Time", "EXOTemp_C_1.5", "EXOCond_uScm_1.5",
                        "EXOSpCond_uScm_1.5", "EXOTDS_mgL_1.5", "EXODOsat_percent_1.5", "EXODO_mgL_1.5", "EXOChla_RFU_1.5",
                        "EXOChla_ugL_1.5", "EXOBGAPC_RFU_1.5", "EXOBGAPC_ugL_1.5", "EXOfDOM_RFU_1.5", "EXOfDOM_QSU_1.5",
                        "EXOTurbidity_FNU_1.5", "EXOTSS_mg_1.5","EXOPressure_psi", "EXODepth_m", "EXOBattery_V",
                        "EXOCablepower_V", "EXOWiper_V", "LvlPressure_psi_13", "LvlTemp_C_13")
  
  
  #Adjustment period of time to stabilization after cleaning in seconds
  ADJ_PERIOD_DO = 2*60*60 
  ADJ_PERIOD_Temp = 30*60
  
  # The if statement is so we can use the function in the visual inspection script if we need
  # to qaqc historical data files from EDI
  
  if(is.character(data_file)){
    # read catwalk data and maintenance log
    # NOTE: date-times throughout this script are processed as UTC
    bvrdata <- read_csv(data_file, skip = 1, col_names = BVRDATA_COL_NAMES,
                        col_types = cols(.default = col_double(), DateTime = col_datetime()))
  } else {
    
    bvrdata <- data_file
  }
  
  #read in manual data from the data logger to fill in missing gaps
  
  if(is.null(data2_file)){
    
    # If there is no manual files then set data2_file to NULL
    bvrdata2 <- NULL
    
  } else{
    
    bvrdata2 <- read_csv(data2_file, skip = 1, col_names = BVRDATA_COL_NAMES,
                         col_types = cols(.default = col_double(), DateTime = col_datetime()))
  }
  
  # Bind the streaming data and the manual downloads together so we can get any missing observations 
  bvrdata <-bind_rows(bvrdata,bvrdata2)%>%
    drop_na(DateTime)
  
  # There are going to be lots of duplicates so get rid of them
  bvrdata <- bvrdata[!duplicated(bvrdata$DateTime), ]
  
  #reorder 
  bvrdata <- bvrdata[order(bvrdata$DateTime),]
  
  # Take out the EXO_Date and EXO_Time column because we don't publish them 
  
  if("EXO_Date" %in% colnames(bvrdata)){
    bvrdata <- bvrdata%>%select(-c(EXO_Date, EXO_Time))
  }
  
  # convert NaN to NAs in the dataframe
  bvrdata[sapply(bvrdata, is.nan)] <- NA
  
  
  ## read in maintenance file 
  log <- read_csv2(maintenance_file, col_types = cols(
    .default = col_character(),
    TIMESTAMP_start = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    TIMESTAMP_end = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    flag = col_integer()
  ))
  
  ### identify the date subsetting for the data
  if (!is.null(start_date)){
    bvrdata <- bvrdata %>% 
      filter(DateTime >= start_date)
    bvrdata2 <- bvrdata2 %>% 
      filter(DateTime >= start_date)
      log <- log %>% 
     filter(TIMESTAMP_start <= end_date)
  }
  
  if(!is.null(end_date)){
    bvrdata <- bvrdata %>% 
      filter(DateTime <= end_date)
    bvrdata2 <- bvrdata2 %>% 
      filter(DateTime <= end_date)
    log <- log %>% 
    filter(TIMESTAMP_end >= start_date)
    
  }
  
  if (nrow(log) == 0){
    log <- log_read
  }
  
  ### add Reservoir and Site columns
  bvrdata$Reservoir="BVR"
  bvrdata$Site=50
  

  #####Create Flag columns#####
  
  
  # for loop to create flag columns
  for(j in colnames(bvrdata%>%select(ThermistorTemp_C_1:LvlTemp_C_13))) { #for loop to create new columns in data frame
    bvrdata[,paste0("Flag_",j)] <- 0 #creates flag column + name of variable
    bvrdata[c(which(is.na(bvrdata[,j]))),paste0("Flag_",j)] <-7 #puts in flag 7 if value not collected
  }
  #update 
  for(k in colnames(bvrdata%>%select(RDO_mgL_6, RDOsat_percent_6,RDO_mgL_13, RDOsat_percent_13,EXOCond_uScm_1.5:EXOTurbidity_FNU_1.5))) { #for loop to create new columns in data frame
    bvrdata[c(which((bvrdata[,k]<0))),paste0("Flag_",k)] <- 3
    bvrdata[c(which((bvrdata[,k]<0))),k] <- 0 #replaces value with 0
  }
  
  
    
  
  #####Maintenance Log QAQC############ 
  
  
  # modify bvrdata based on the information in the log   
  
  for(i in 1:nrow(log))
  {
    ### get start and end time of one maintenance event
    start <- log$TIMESTAMP_start[i]
    end <- log$TIMESTAMP_end[i]
    
    
    ### Get the Reservoir
    
    Reservoir <- log$Reservoir[i]
    
    ### Get the Site
    
    Site <- log$Site[i]
    
    ### Get the Maintenance Flag 
    
    flag <- log$flag[i]
    
    ### Get the update_value that an observation will be changed to 
    
    update_value <- as.numeric(log$update_value[i])
    
    ### Get the adjustment_code for a column to fix a value. If it is not an NA
    
    if(flag==8){
      # These update_values are expressions so they should not be set to numeric
      adjustment_code <- log$adjustment_code[i]
      
    }else{
      adjustment_code <- as.numeric(log$adjustment_code[i])
    }
    
    
    ### Get the names of the columns affected by maintenance
 
    colname_start <- log$start_parameter[i]
    colname_end <- log$end_parameter[i]
    
    ### if it is only one parameter parameter then only one column will be selected
    
    if(is.na(colname_start)){
      
      maintenance_cols <- colnames(bvrdata%>%select(any_of(colname_end))) 
      
    }else if(is.na(colname_end)){
      
      maintenance_cols <- colnames(bvrdata%>%select(any_of(colname_start)))
      
    }else{
      maintenance_cols <- colnames(bvrdata%>%select(c(colname_start:colname_end)))
    }
   
    
    ### Get the name of the flag column
    
    flag_cols <- paste0("Flag_", maintenance_cols)
    
    
    ### Getting the start and end time vector to fix. If the end time is NA then it will put NAs 
    # until the maintenance log is updated
    
    if(is.na(end)){
      # If there the maintenance is on going then the columns will be removed until
      # and end date is added
      Time <- bvrdata$DateTime >= start
      
    }else if (is.na(start)){
      # If there is only an end date change columns from beginning of data frame until end date
      Time <- bvrdata$DateTime <= end
      
    }else {
      
      Time <- bvrdata$DateTime >= start & bvrdata$DateTime <= end
      
    }
    
    # replace relevant data with NAs and set flags while maintenance was in effect
    if (flag==1){
      # The observations are changed to NA for maintenance or other issues found in the maintenance log
      bvrdata[Time, maintenance_cols] <- NA
      bvrdata[Time, flag_cols] <- flag
      
    } else if (flag==2){
                if(!is.na(adjustment_code)){
         
          # Add a flag based on the conditions in the maintenance log
          eval(parse(text=paste0(adjustment_code, "flag_cols] <- flag")))
          # Change to NA based on the conditions in the maintenance log
          eval(parse(text=paste0(adjustment_code, "maintenance_cols] <- NA")))
          
        }else{

      # The observations are changed to NA for maintenance or other issues found in the maintenance log
      bvrdata[Time, maintenance_cols] <- NA
      bvrdata[Time, flag_cols] <- flag
      }
      ## Flag 3 is removed in the for loop before the maintenance log where negative values are changed to 0
      
    } else if (flag==4){
      
        # Set the values to NA and flag
        bvrdata[Time, maintenance_cols] <- NA
        bvrdata[Time, flag_cols] <- flag

    } else if (flag==5){
      
        # Values are flagged but left in the dataset
        bvrdata[Time, flag_cols] <- flag

    } else if (flag==6){ #adjusting the conductivity based on the equation in the maintenance log 
      
      if (maintenance_cols %in% c("EXOCond_uScm_1.5", "EXOSpCond_uScm_1.5", "EXOTDS_mgL_1.5")){
        
        bvrdata[Time, maintenance_cols] <- eval(parse(text=adjustment_code))
        
        bvrdata[Time, flag_cols] <- flag
    
    }else if (flag==7){
      # Data was not collected and already flagged as NA above 
      
    }else{
      # Flag is not in Maintenance Log
      warning(paste0("Flag ", flag, " used not defined in the L1 script. 
                     Talk to Austin and Adrienne if you get this message"))
    }
    
    # Add the 2 hour adjustment for DO. This means values less than 2 hours after the DO sensor is out of the water are changed to NA and flagged
    # In 2023 added a 30 minute adjustment for Temp sensors on the temp string  
    
    # Make a vector of the DO columns
    
    DO <- colnames(bvrdata%>%select(grep("DO_mgL|DOsat", colnames(bvrdata))))
    
    # Vector of thermistors on the temp string 
    Temp <- colnames(bvrdata%>%select(grep("Thermistor|RDOTemp|LvlTemp", colnames(bvrdata))))
    
    # make a vector of the adjusted time
    Time_adj_DO <- bvrdata$DateTime>start&bvrdata$DateTime<end+ADJ_PERIOD_DO
    
    Time_adj_Temp <- bvrdata$DateTime>start&bvrdata$DateTime<end+ADJ_PERIOD_Temp
    
    # Change values to NA after any maintenance for up to 2 hours for DO sensors
    
    if (flag ==1 ){
      
      # This is for DO add a 2 hour buffer after the DO sensor was out of the water
      bvrdata[Time_adj_DO,  maintenance_cols[maintenance_cols%in%DO]] <- NA
      bvrdata[Time_adj_DO, flag_cols[flag_cols%in%DO]] <- flag
      
      # Add a 30 minute buffer for when the temp string was out of the water
      bvrdata[Time_adj_Temp,  maintenance_cols[maintenance_cols%in%Temp]] <- NA
      bvrdata[Time_adj_Temp, flag_cols[flag_cols%in%Temp]] <- flag
      }
    }
  }    
  
  ############## Remove and Flag when sensors are out of position ####################
  
  #change EXO values to NA if EXO depth is less than 0.5m and Flag as 2
  
  #index only the colummns with EXO at the beginning
  exo_idx <-grep("^EXO",colnames(bvrdata))
  
  
  #create list of the Flag columns that need to be changed to 2
  exo_flag <- grep("^Flag_EXO*$",colnames(bvrdata))
  
  #Change the EXO data to NAs when the EXO is above 0.2m and not due to maintenance
  #Flag the data that was removed with 2 for outliers
  bvrdata[which(bvrdata$EXODepth_m<0.2),exo_flag]<- 2
  bvrdata[which(bvrdata$EXODepth_m < 0.2), exo_idx] <- NA
  
  # Flag the EXO data when the wiper isn't parked in the right position because it could be on the sensor when taking a reading
  #Flag the data that was removed with 2 for outliers
  bvrdata[which(bvrdata$EXOWiper_V !=0 & bvrdata$EXOWiper_V < 0.7 & bvrdata$EXOWiper_V > 1.6),exo_flag]<- 2
  bvrdata[which(bvrdata$EXOWiper_V !=0 & bvrdata$EXOWiper_V < 0.7 & bvrdata$EXOWiper_V > 1.6), exo_idx] <- NA
 
  
  #change the temp string and pressure sensor to NA if the psi is less than XXXXX and Flag as 2
  
  #index only the colummns with EXO at the beginning
  temp_idx <-grep("^Ther*|^RDO*|^Lvl*",colnames(bvrdata))
  
  #create list of the Flag columns that need to be changed to 2
  temp_flag <- grep("^Flag_Ther*|^Flag_RDO*|^Flag_Lvl*",colnames(bvrdata))
  
  #Change the EXO data to NAs when the pressure sensor is less than 10 psi which is roughly 7m and not due to maintenance. 
  # Also remove when the pressure sensor is NA because we don't know at what depth the sensors are at. 
  bvrdata[which(bvrdata$LvlPressure_psi_13 < 10), temp_flag]<- 2
  bvrdata[which(bvrdata$LvlPressure_psi_13 < 10), temp_idx] <- NA
  #Flag the data that was removed with 2 for outliers
  
  # If the pressure sensor is reading NA then have to take out all readings because we can't get a depth
  
  bvrdata[which(is.na(bvrdata$LvlPressure_psi_13)), temp_flag]<-2
  bvrdata[which(is.na(bvrdata$LvlPressure_psi_13)), temp_idx]<-NA
  
  
  ############## Leading and Lagging QAQC ##########################
  # This finds the point that is way out of range from the leading and lagging point 
  
  # loops through all of the columns to catch values that are above 2 or 4 sd above or below
  # the leading or lagging point 
  
  # need to make it a data frame because I was having issues with calculating the mean
  
  bvrdata=data.frame(bvrdata)
  
  for (a in colnames(bvrdata%>%select(ThermistorTemp_C_1:EXOTurbidity_FNU_1.5, LvlPressure_psi_13:LvlTemp_C_13))){
    Var_mean <- mean(bvrdata[,a], na.rm = TRUE)
    
    # For Algae sensors we use 4 sd as a threshold but for the others we use 2
    if (colnames(bvrdata[a]) %in% c("EXOChla_RFU_1.5","EXOChla_ugL_1.5","EXOBGAPC_RFU_1.5","EXOBGAPC_ugL_1.5")){
      Var_threshold <- 4 * sd(bvrdata[,a], na.rm = TRUE)
    }else{ # all other variables we use 2 sd as a threshold
      Var_threshold <- 2 * sd(bvrdata[,a], na.rm = TRUE)
    }
    # Create the observation column, the lagging column and the leading column
    bvrdata$Var <- lag(bvrdata[,a], 0)
    bvrdata$Var_lag = lag(bvrdata[,a], 1)
    bvrdata$Var_lead = lead(bvrdata[,a], 1)
    
    # Replace the observations that are above the threshold with NA and then put a flag in the flag column
    
    bvrdata[c(which((abs(bvrdata$Var_lag - bvrdata$Var) > Var_threshold) &
                      (abs(bvrdata$Var_lead - bvrdata$Var) > Var_threshold)&!is.na(bvrdata$Var))) ,a] <-NA
    
    bvrdata[c(which((abs(bvrdata$Var_lag - bvrdata$Var) > Var_threshold) &
                      (abs(bvrdata$Var_lead - bvrdata$Var) > Var_threshold)&!is.na(bvrdata$Var))) ,paste0("Flag_",colnames(bvrdata[a]))]<-2
  }
  
  # Remove the leading and lagging columns
  
  bvrdata<-bvrdata%>%select(-c(Var, Var_lag, Var_lead))
  
  ### Remove observations when sensors are out of the water ###
  
  #create depth column
  bvrdata=bvrdata%>%mutate(Depth_m_13=LvlPressure_psi_13*0.70455)#1psi=2.31ft, 1ft=0.305m
  
  
  # Using the find_depths function
  
  bvrdata2 <- find_depths (data_file = bvrdata, # data_file = the file of most recent data either from EDI or GitHub. Currently reads in the L1 file
                          depth_offset = "https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data-qaqc/BVR_Depth_offsets.csv",  # depth_offset = the file of depth of each sensor relative to each other. This file for BVR is on GitHub
                          output = NULL, # output = the path where you would like the data saved
                          date_offset = "2021-04-05", # Date_offset = the date we moved the sensors so we know where to split the file. If you don't need to split the file put NULL
                          offset_column1 = "Offset_before_05APR21",# offset_column1 = name of the column in the depth_offset file to subtract against the actual depth to get the sensor depth
                          offset_column2 = "Current_offset", # offset_column2 = name of the second column if applicable for the column with the depth offsets
                          round_digits = 2, #round_digits = number of digits you would like to round to
                          bin_width = 0.25, # bin width in m
                          wide_data = T)  
  
  # Flag observations that were removed but don't have a flag yet
  
  for(j in colnames(bvrdata2%>%select(ThermistorTemp_C_1:EXOWiper_V))) { #for loop to create new columns in data frame
    bvrdata2[c(which(is.na(bvrdata2[,j]) & bvrdata2[,paste0("Flag_",j)]==0)),paste0("Flag_",j)] <-2 #put a flag of 2 for observations out of the water
  }
  
  #### Organize the file for saving########  
  
  
  
  # reorder columns
  bvrdata2 <- bvrdata2 %>% select(Reservoir, Site, DateTime, ThermistorTemp_C_1:ThermistorTemp_C_13,
                                RDO_mgL_6, RDOsat_percent_6,
                                RDOTemp_C_6, RDO_mgL_13, RDOsat_percent_13, RDOTemp_C_13,
                                EXOTemp_C_1.5, EXOCond_uScm_1.5, EXOSpCond_uScm_1.5, EXOTDS_mgL_1.5, EXODOsat_percent_1.5,
                                EXODO_mgL_1.5, EXOChla_RFU_1.5, EXOChla_ugL_1.5, EXOBGAPC_RFU_1.5, EXOBGAPC_ugL_1.5,
                                EXOfDOM_RFU_1.5, EXOfDOM_QSU_1.5,EXOTurbidity_FNU_1.5, EXOPressure_psi, EXODepth_m, EXOBattery_V, EXOCablepower_V,
                                EXOWiper_V, LvlPressure_psi_13, Depth_m_13, LvlTemp_C_13, RECORD, CR6Battery_V, CR6Panel_Temp_C,
                                Flag_ThermistorTemp_C_1:Flag_ThermistorTemp_C_13,Flag_RDO_mgL_6, Flag_RDOsat_percent_6, Flag_RDOTemp_C_6,
                                Flag_RDO_mgL_13, Flag_RDOsat_percent_13, Flag_RDOTemp_C_13,Flag_EXOTemp_C_1.5, Flag_EXOCond_uScm_1.5, Flag_EXOSpCond_uScm_1.5,Flag_EXOTDS_mgL_1.5,
                                Flag_EXODOsat_percent_1.5, Flag_EXODO_mgL_1.5, Flag_EXOChla_RFU_1.5,Flag_EXOChla_ugL_1.5, Flag_EXOBGAPC_RFU_1.5,Flag_EXOBGAPC_ugL_1.5,
                                Flag_EXOfDOM_RFU_1.5,Flag_EXOfDOM_QSU_1.5, Flag_EXOTurbidity_FNU_1.5, 
                                Flag_EXOPressure_psi, Flag_EXODepth_m, Flag_EXOBattery_V, Flag_EXOCablepower_V,Flag_EXOWiper_V,Flag_LvlPressure_psi_13, Flag_LvlTemp_C_13)
  
  #order by date and time
  bvrdata2 <- bvrdata2[order(bvrdata2$DateTime),]
  
  
  # convert datetimes to characters so that they are properly formatted in the output file
  bvrdata2$DateTime <- as.character(bvrdata2$DateTime)
  
  # subset to only the current year when using for EDI publishing
  # current_time_end is set in Chunk 1 Set Up in Inflow_QAQC_Plots_2013_2022.Rmd
  # if(is.null(start_date)){
  #   bvrdata <- bvrdata[bvrdata$DateTime<ymd_hms(current_time_end),]
  # }
  
  # write to output file
  write_csv(bvrdata2, output_file)
  
} 


# example usage


#qaqc('https://github.com/FLARE-forecast/BVRE-data/raw/bvre-platform-data/BVRplatform.csv',
#      'https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/BVRplatform_manual_2020.csv',
#      "https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data/BVR_maintenance_log.txt",
#       "BVRplatform_clean.csv", 
#     "BVR_Maintenance_2020.csv")


