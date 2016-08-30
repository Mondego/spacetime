#  MultiProcessing : True   MaximumTravelers : 5000   Interval : 0.2   Timer: 0:20:00	 Store: PythonRemoteStore
library(Hmisc)
library(TTR)
library(xtable)
plotted = FALSE
benchmark_parser <- function(fpath, draw){
    sptable <- read.csv(file=fpath, header=TRUE, sep=",", fileEncoding="UTF-8", na.strings='NULL', skip=4)


    sptable = subset(sptable, update_delta < unname(quantile(sptable$update_delta, c(0.99))))
    sptable$convtime = strptime(sptable$time, "%H:%M:%OS")

    # moving average
    #los = EMA(os$delta, 20)
    #lsumo = EMA(sumo$delta, 20)
    #lsocial = EMA(social$delta, 20)

    if (draw == TRUE) {
        if (!plotted) {
            print("Plotting graph for first time!")
            plot(sptable$convtime, sptable$update_delta, col='white', ylim=c(0,180), col.lab=1.25, cex.lab=1.25, xlab="experiment duration", ylab="processing time / step")
            legend('topleft', legend=c('avg update'), col=c('blue'), lty=1, bty='n', cex=1.25)
            assign("plotted", TRUE, envir = .GlobalEnv)
        }
        lines(sptable$convtime, sptable$update_delta, col='blue')
    }
    result <- list(table=sptable, mean_looptime=mean(sptable$update_delta), stdev_looptime=sd(sptable$update_delta),
                   mean_pull=mean(sptable$X__pull), stdev_pull=sd(sptable$X__pull),
                   mean_push=mean(sptable$X__push), stdev_push=sd(sptable$X__push),
                   mean_bytes=mean(sptable$bytes.sent), stdev_bytes=sd(sptable$bytes.sent),
                   mean_byter=mean(sptable$bytes.received), stdev_byter=sd(sptable$bytes.received),
                   mean_update=mean(sptable$update), stdev_update=sd(sptable$update)
                   )
}

generate_graph <- function(ylist, lgd_names, lgd_colors, fname, width=8, height=4.5) {
    #pdf(fname,width=width,height=height)
    png(paste(fname,"png",sep='.'),width=width,height=height,units="in",res=300)
    par(mar=c(4.5, 4.0, 1.0,1.0))
    plot(ylist[[1]], col='white', ylim=c(0,1300), col.lab=1.25, cex.lab=1.25, xlab="simulation step", ylab="processing time(ms)")
    #plot(ylist[[1]], col='white', col.lab=1.25, cex.lab=1.25, xlab="simulation step", ylab="processing time(ms)")
    legend("topright", legend=unlist(lgd_names), col=unlist(lgd_colors), lty=1, bty='n', cex=1, y.intersp=1.35)
    for (i in 1:length(ylist)) {
        lines(ylist[[i]], col=lgd_colors[[i]])
    }
    dev.off()
}