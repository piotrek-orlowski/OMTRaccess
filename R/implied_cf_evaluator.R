implied_cf_evaluator <-
function(u_matrix, t.vec, N.factors, mkt, ...){
  u_vec <- Im(u_matrix[,1])
  cf_eval <- tpsImpliedCF(option.panels = NULL
                          , mkt.frame = NULL
                          , u.t.mat = expand.grid(u=u_vec, t = mkt$t)
                          , discounted = FALSE
                          , doPlot = 0
                          , ...
  )
  colnames(cf_eval)[3] <- "cf"
  return(cf_eval$cf)
}
