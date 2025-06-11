#script written to create daily figures that are sent to lucky SCC team members :)
#written by CCC, BB, Vahid-Dan, ABP

# Edits:
# 20 May 2025 - just BVR plot
# 18 March 2024- add section for eddyflux fluxes
# 29 Sept. 2024- added in the water level to CCR
# 09 Dec. 2024 - added in an if statment to read in BVR file because added back in the data logger header. Add print statment when plots printed. 

continue_on_error <- function()
{
  print("ERROR! CONTINUING WITH THE REST OF THE SCRIPT ...")
}

options(error=continue_on_error)

#loading packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(Rcpp, generics, lubridate, tidyverse,gridExtra,openair, dplyr, magrittr, ggplot2)

# Set the timeout option to 500 seconds instead of 60
options(timeout=500)

# download data file
download.file('https://github.com/FLARE-forecast/BVRE-data/raw/bvre-platform-data/bvre-waterquality.csv','bvre-waterquality.csv')

#time to now play with BVR data!
bvrdata=0 #create something so we know if it got read in or not
bvrdata<-read.csv("bvre-waterquality.csv", as.is=T) #wonky Campbell rows were deleted
#bvrdata<-read.csv("bvre-waterquality.csv", skip=4, header=F) #get data minus wonky Campbell rows
#names(bvrdata)<-names(bvrheader) #combine the names to deal with Campbell logger formatting

#if statement for when there is a header from the data logger for the header
if(bvrdata==0){
  bvrheader <- read.csv("bvre-waterquality.csv", skip=1, as.is=T) #get header minus wonky Campbell rows
  bvrdata<-read.csv("bvre-waterquality.csv", skip=4, header = F)#get data minus wonky Campbell rows
  names(bvrdata)<-names(bvrheader) #combine the names to deal with Campbell logger formatting
}

bvrdata=bvrdata%>%
  filter(grepl("^20", TIMESTAMP))%>% #keep only the right TIMESTAMP rows 
  distinct(TIMESTAMP, .keep_all= TRUE) #taking out the duplicate values 

end.time1 <- with_tz(as.POSIXct(strptime(Sys.time(), format = "%Y-%m-%d %H")), tzone = "Etc/GMT+5") #gives us current time with rounded hours in EDT
start.time1 <- end.time1 - days(7) #to give us seven days of data for looking at changes
full_time1 <- as.data.frame(seq(start.time1, end.time1, by = "10 min")) #create sequence of dates from past 7 days to fill in data for a data frame
colnames(full_time1)=c("TIMESTAMP") #make it a data frame to merge to make obs1 later

#obs3 <- array(NA,dim=c(length(full_time1),44)) #create array that will be filled in with 44 columns (the entire size of the array)
bvrdata$TIMESTAMP<-as.POSIXct(strptime(bvrdata$TIMESTAMP, "%Y-%m-%d %H:%M"), tz = "Etc/GMT+5") #get dates aligned


if (length(na.omit(bvrdata$TIMESTAMP[bvrdata$TIMESTAMP>start.time1]))<2) { #if there is no data after start time, then a pdf will be made explaining this
  pdf(paste0("BVRDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  plot(NA, xlim=c(0,5), ylim=c(0,5), bty='n',xaxt='n', yaxt='n', xlab='', ylab='') #creates empty plot
  mtext(paste("No data found between", start.time1, "and", end.time1, sep = " ")) #fills in text in top margin of plot
  dev.off() #file made!
  print("BVR file made with no data")
} else {
  
  obs3 <- merge(full_time1,bvrdata, all.x=TRUE)#merge the data frame to get the last 7 days
  
  obs3$Lvl_psi<-as.numeric(obs3$Lvl_psi) # convert to numeric
  
  obs3$Depth_m=obs3$Lvl_psi*0.70455 #converts pressure to depth
  
  pdf(paste0("BVRDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  par(mfrow=c(3,2))
  
  plot(obs3$TIMESTAMP,obs3$BattV, main="Campbell Logger Battery", xlab="Time", ylab="Volts", type='l')
  if(min(tail(na.omit(obs3$BattV)))<11.5){
    mtext("Battery Charge Low", side = 3, col="red")
  }
  #added y limits so the axises would show up when the are no data
  
  plot(obs3$TIMESTAMP,obs3$EXO_battery, main="EXO Battery", xlab="Time", ylab="Volts", type='l', ylim=c(3,7))
  plot(obs3$TIMESTAMP,obs3$EXO_cablepower, main="EXO Cable Power", xlab="Time", ylab="Volts", type='l', ylim=c(10,15))
  
  plot(obs3$TIMESTAMP,obs3$EXO_depth, main="EXO Depth", xlab="Time", ylab="Meters", type='l', ylim=c(0,4))
  plot(obs3$TIMESTAMP, obs3$Depth_m, main="Bottom Sensor Depth", xlab="Time", ylab="Meters",type='l')
  plot(obs3$TIMESTAMP, obs3$EXO_wiper, main= "Wiper Voltage", xlab="Time", ylab="Volts", type='l')
  
  plot(obs3$TIMESTAMP,obs3$EXO_pressure, main="Sonde Pressure", xlab="Time", ylab="psi", type='l', ylim=c(-1,22))
  points(obs3$TIMESTAMP, obs3$Lvl_psi, col="blue4", type='l')
  legend("topleft", c("1.5m EXO", "11m PT"), text.col=c("black", "blue4"), x.intersp=0.001)
  
  par(mar=c(5.1, 4.1, 4.1, 2.1), mgp=c(3, 1, 0), las=0)
  plot(obs3$TIMESTAMP,obs3$dotemp_13, main="Water temp of sondes", xlab="Time", ylab="degrees C", type='l', col="medium sea green", lwd=1.5, ylim=c(min(obs3$dotemp_13, obs3$wtr_pt_13, na.rm = TRUE) - 1, max(obs3$EXO_wtr_1, na.rm = TRUE) + 5))
  points(obs3$TIMESTAMP, obs3$dotemp_6, col="black", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$EXO_wtr_1, col="magenta", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_pt_13, col="blue4", type='l', lwd=1.5)
  legend("topleft", c("1.5m EXO", "4m DO", "11m DO", "11m PT"), text.col=c("magenta", "black", "medium sea green", "blue4"), x.intersp=0.001)
  
  plot(obs3$TIMESTAMP,obs3$doobs_13, main="DO", xlab="Time", ylab="mg/L", type='l', col="medium sea green", lwd=1.5, ylim=c(min(obs3$doobs_13, obs3$doobs_6, obs3$doobs_1, na.rm = TRUE) - 1, max(obs3$doobs_13, obs3$doobs_6, obs3$doobs_1, na.rm = TRUE) + 2))
  points(obs3$TIMESTAMP, obs3$doobs_6, col="black", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$doobs_1, col="magenta", type='l', lwd=1.5)
  legend("topleft", c("1.5m", "4m", "11m"), text.col=c("magenta", "black", "medium sea green"), x.intersp=0.001)
  
  plot(obs3$TIMESTAMP,obs3$dosat_13, main="DO % saturation", xlab="Time", ylab="% saturation", type='l', col="medium sea green", lwd=1.5, ylim=c(min(obs3$dosat_13, obs3$dosat_6, obs3$dosat_1, na.rm = TRUE) - 1, max(obs3$dosat_13, obs3$dosat_6, obs3$dosat_1, na.rm = TRUE) + 5))
  points(obs3$TIMESTAMP, obs3$dosat_6, col="black", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$dosat_1, col="magenta", type='l', lwd=1.5)
  legend("topleft", c("1.5m", "4m", "11m"), text.col=c("magenta", "black", "medium sea green"), x.intersp=0.001)
  
  plot(obs3$TIMESTAMP,obs3$Cond_1, main="Cond, SpCond, TDS @ 1.5m", xlab="Time", ylab="uS/cm or mg/L", type='l', col="red", lwd=1.5, ylim=c(-0.5,45))
  points(obs3$TIMESTAMP, obs3$SpCond_1, col="black", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$TDS_1, col="orange", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$Turbidity_FNU_1, col="brown", type='l', lwd=1.5)
  legend("topleft", c("TDS", "SpCond", "Cond", "Turbidity"), text.col=c("orange", "black","red", "brown"), x.intersp=0.001)
  
  plot(obs3$TIMESTAMP,obs3$Chla_1, main="Chla, Phyco, fDOM", xlab="Time", ylab="ug/L or QSU", type='l', col="green", lwd=1.5, ylim=c(-0.5,30))
  points(obs3$TIMESTAMP, obs3$BGAPC_1, col="blue", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$fDOM_QSU_1, col="firebrick4", type='l', lwd=1.5)
  legend("topleft", c("Chla", "Phyco", "fDOM"), text.col=c("green", "blue", "firebrick4"), x.intersp=0.001)
  
  par(mfrow=c(1,1))
  par(oma=c(1,1,1,4))
  plot(obs3$TIMESTAMP,obs3$PTemp_C, main="Water Temp", xlab="Time", ylab="degrees C", type='l', col="black", lwd=1.5, ylim=c(0,40))
  points(obs3$TIMESTAMP,obs3$wtr_1, col="firebrick4", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_2, col="firebrick1", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_3, col="DarkOrange1", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_4, col="gold", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_5, col="greenyellow", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_6, col="medium sea green", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_7, col="sea green", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_8, col="DeepSkyBlue4", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_9, col="blue2", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_10, col="blue4", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_11, col="darkslateblue", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_12, col="magenta2", type='l', lwd=1.5)
  points(obs3$TIMESTAMP, obs3$wtr_13, col="darkmagenta", type='l', lwd=1.5)
  
  #subset if there are missing pressure values
  obs3_sub=obs3[!(is.na(obs3$Lvl_psi) | obs3$Lvl_psi==""), ]
  
  par(fig=c(0,1,0,1), oma=c(0,0,0,0), mar=c(0,0,0,0), new=T)
  plot(0, 0, type = "n", bty = "n", xaxt = "n", yaxt = "n")
  #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  if(as.numeric(obs3_sub[1,45])<14.5 & as.numeric(obs3_sub[1,45])>14){
    legend("right",c("Air", "Air", "Air", "0.1m", "1m", "2m", "3m", "4m","5m", "6m", "7m", "8m", "9m", "10m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  }else if(as.numeric(obs3_sub[1,45])<15.3 & as.numeric(obs3_sub[1,45])>14.5){
    legend("right",c("Air", "Air", "Air", "0.5m", "1.5m", "2.5m", "3.5m", "4.5m","5.5m", "6.5m", "7.5m", "8.5m", "9.5m", "10.5m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<16 & as.numeric(obs3_sub[1,45])>15.3){
    legend("right",c("Air", "Air", "Air", "1m", "2m", "3m", "4m", "5m","6m", "7m", "8m", "9m", "10m", "11m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<16.7 & as.numeric(obs3_sub[1,45])>16){
    legend("right",c("Air", "Air", "0.5", "1.5m", "2.5m", "3.5m", "4.5m", "5.5m","6.5m", "7.5m", "8.5m", "9.5m", "10.5m", "11.5m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<17.4 & as.numeric(obs3_sub[1,45])>16.7){
    legend("right",c("Air", "Air", "1m", "2m", "3m", "4m", "5m", "6m","7m", "8m", "9m", "10m", "11m", "12m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<18.1 & as.numeric(obs3_sub[1,45])>17.4) {
    legend("right",c("Air", "0.5m", "1.5m", "2.5m", "3.5m", "4.5m", "5.5m", "6.5m","7.5m", "8.5m", "9.5m", "10.5m", "11.5m", "12.5m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI__psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<14 & as.numeric(obs3_sub[1,45])>13.2){
    legend("right",c("Air", "Air", "Air", "Air","0.5m", "1.5m", "2.5m", "3.5m", "4.5m", "5.5m", "6.5m","7.5m", "8.5m", "9.5m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<13.2 & as.numeric(obs3_sub[1,45])>12.4){
    legend("right",c("Air", "Air", "Air", "Air","Air", "1m", "2m", "3m", "4m", "5m", "6m","7m", "8m", "9m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<12.4 & as.numeric(obs3_sub[1,45])>11.9){
    legend("right",c("Air", "Air", "Air", "Air", "Air", "0.5m", "1.5m", "2.5m", "3.5m", "4.5m", "5.5m", "6.5m","7.5m", "8.5m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else if (as.numeric(obs3_sub[1,45])<11.9 & as.numeric(obs3_sub[1,45])>11){
    legend("right",c("Air", "Air", "Air", "Air", "Air", "Air", "1m", "2m", "3m", "4m", "5m", "6m","7m", "8m"),
           text.col=c("black","firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                      "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    #using the first psi reading from LVI_psi which is at the bottom of the string  to determine the depth of the sensors
  } else  {
    legend("right",c("Out of Range"),
           text.col=c("black"),
           cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
    
  }
  dev.off() #file made!
  print("BVR file made with all data")
}
