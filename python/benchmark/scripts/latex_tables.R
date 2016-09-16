library(xtable)

sumo_odelta = mean(sumo_original$HandleEvent)
os_odelta = mean(os_original$HandleEvent)
social_odelta = mean(social_original$HandleEvent)

sumo_fdelta = mean(sumo_frame$delta)
os_fdelta = mean(os_frame$delta)
social_fdelta = mean(social_frame$delta)

sumo_push = mean(sumo_frame$push)
opensim_push = mean(os_frame$push)
social_push = mean(social_frame$push)

sumo_pull = mean(sumo_frame$pull)
opensim_pull = mean(os_frame$pull)
social_pull = mean(social_frame$pull)

sumo_update = mean(sumo_frame$update)
opensim_update = mean(os_frame$update)
social_update = mean(social_frame$update)

dframe = data.frame(
  'push'=c(sumo_push, opensim_push, social_push),
  'pull'=c(sumo_pull, opensim_pull, social_pull),
  'update'=c(sumo_update, opensim_update, social_update))
allframe = data.frame(
  'Event-based'=c(sumo_odelta, os_odelta, social_odelta),
  'CADIS'=c(sumo_fdelta, os_fdelta, social_fdelta)
  )

row.names(dframe) <- c("SumoConnector", "OpensimConnector", "SocialConnector")
row.names(allframe) <- c("SumoConnector", "OpensimConnector", "SocialConnector")

xtable(allframe)
xtable(dframe)
