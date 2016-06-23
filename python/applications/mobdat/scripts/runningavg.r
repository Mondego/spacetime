#  MultiProcessing : True   MaximumTravelers : 5000	 Interval : 0.2	 Timer: 0:20:00	 Store: PythonRemoteStore
library(Hmisc)
library(TTR)
setwd("/home/arthur/Dropbox/workspace/mobdat/stats")
osfn = "latest_OpenSimConnector"
sumofn = "latest_SumoConnector"
socialfn = "latest_SocialConnector"
os <- read.csv(file=osfn, header=TRUE, sep=",", fileEncoding="UTF-8-BOM", na.strings='NULL', skip=4)
sumo <- read.csv(file=sumofn, header=TRUE, sep=",", fileEncoding="UTF-8-BOM", na.strings='NULL', skip=4)
social <- read.csv(file=socialfn, header=TRUE, sep=",", fileEncoding="UTF-8-BOM", na.strings='NULL', skip=4)
#time,delta,nobjects,mem buffer,vehicles,pull,push,_FindAssetInObject,
#HandleCreateObjectEvent,HandleDeleteObjectEvent,HandleObjectDynamicsEvent,update

os$convtime = strptime(os$time, "%H:%M:%OS")
sumo$convtime = strptime(sumo$time, "%H:%M:%OS")
social$convtime = strptime(social$time, "%H:%M:%OS")
#head(d)
# remove outlier delta
#hist(d$delta,breaks=seq(0,2500,10))
os_trim = subset(os, delta < 1000)
sumo_trim = subset(sumo, delta < 1000)
social_trim = subset(social, delta < 1000)

plot(os_trim$convtime, os_trim$delta, col='white', ylim=c(0,500))
#lines(lowess(d2$convtime, d2$delta),col='red')

# moving average
los = EMA(os_trim$delta, 20)
lsumo = EMA(sumo_trim$delta, 20)
lsocial = EMA(social_trim$delta, 20)

lines(os_trim$convtime,los, col='blue')
lines(sumo_trim$convtime,lsumo, col='green')
lines(social_trim$convtime,lsocial, col='orange')

length(sumo_trim$convtime)

legend('topright', legend=c('opensim','sumo','social'), col=c('blue','green','orange'), lty=1)