#' Title Implied volatility with American option DB
#'
#' @param option_data 
#' @param option_distributions 
#' @param option_risk_free_rates 
#' @param security_data 
#' @param save_path 
#' @param save_filename 
#' @param cl 
#'
#' @return
#' @export
#'
#' @examples
calculate_implied_volatility <- function(option_data, option_distributions, option_risk_free_rates, security_data, save_path, save_filename, cl){
  
  # Modify function
  # - first join all
  # - then partition
  # - move from NMOF to RQuantLib
  #   - calculate yield implied by payoffs
  
  loc_env <- environment()
  
  parallel::clusterEvalQ(cl
                         , impl_vol_vectorized <- function(exercise, M, uniroot.control, ...){
                           # , price, S, X, tau, r, q, tauD, D
                           mapply(function(...) tryCatch(NMOF::vanillaOptionImpliedVol(...), error = function(e) NA_real_)
                                  , ...
                                  , MoreArgs = list(exercise = exercise
                                                    , uniroot.control = uniroot.control
                                                    , M = M
                                                    )
                                  , SIMPLIFY = TRUE
                                  , USE.NAMES = FALSE)
                           })
    
  
  parallel::clusterExport(cl, c("option_distributions", "option_risk_free_rates", "security_data", "save_path", "save_filename"), envir = loc_env)
  
  print("Finished cluster export")
  
  option_data <- option_data %>% 
    dplyr::mutate(year = lubridate::year(date), month = lubridate::month(date))
  
  option_data <- multidplyr::partition(option_data %>% dplyr::ungroup(), year, month, secid, cluster = cl)
  
  option_and_iv_data <- option_data %>% 
            dplyr::do({
              # out_data <- dplyr::do(dplyr::group_by(.,date, strike_price, exdate, cp_flag),{
              out_data <- dplyr::do(.,{
                loc_data <- .
                loc_data <- dplyr::arrange(loc_data, date, optionid)
                # join with distributions Tue Oct 16 15:03:51 2018 ------------------------------
                loc_distr_data <- dplyr::left_join(loc_data %>% dplyr::select(secid, date, exdate, optionid), option_distributions)
                # join with risk-free rates Tue Oct 16 15:06:04 2018 ------------------------------
                loc_data <- dplyr::left_join(loc_data, option_risk_free_rates)
                # join with price data Tue Oct 16 15:08:01 2018 ------------------------------
                loc_data <- dplyr::left_join(loc_data, security_data %>% dplyr::select(secid,date,close))
                # locate NAs in distribution (there might be NAs if no dividends before maturity) Tue Oct 16 16:52:16 2018 ------------------------------
                loc_distr_data <- dplyr::mutate_if(loc_distr_data, .predicate = is.numeric, .funs = dplyr::funs(dplyr::if_else(is.na(.),0,.))) %>% distinct() %>% arrange(date, optionid)
                
                tauD_list <- loc_distr_data %>% plyr::dlply(.variables=c("date","optionid"), .fun = function(x) x %>% pull(time_to_distribution))
                D_list <- loc_distr_data %>% plyr::dlply(.variables=c("date","optionid"), .fun = function(x) x %>% pull(dividend_amount))
                # calculate IV Tue Oct 16 15:53:22 2018 ------------------------------
                loc_data <- dplyr::mutate(loc_data
                                          ,impl_volatility_manual = impl_vol_vectorized(exercise = "american"
                                                                                        , price = 0.5*(loc_data$best_bid+loc_data$best_offer)
                                                                                        , S = loc_data$close
                                                                                        , X = loc_data$strike_price/1000
                                                                                        , tau = loc_data$time_to_maturity
                                                                                        , r = loc_data$risk_free_rate
                                                                                        , q = 0
                                                                                        , tauD = tauD_list
                                                                                        , D = D_list
                                                                                        , type = dplyr::if_else(loc_data$cp_flag=="P","put","call")
                                                                                        , M = 350
                                                                                        , uniroot.control = list(interval = c(1e-2,2.5))
                                                                                        )
                                          )
                # calculate bid/ask true IV Tue Oct 16 16:07:40 2018 ------------------------------
                loc_data <- dplyr::mutate(loc_data
                                          , impl_volatility_manual_bid = impl_vol_vectorized(exercise = "american"
                                                                                             , price = best_bid
                                                                                             , S = loc_data$close
                                                                                             , X = loc_data$strike_price/1000
                                                                                             , tau = loc_data$time_to_maturity
                                                                                             , r = loc_data$risk_free_rate
                                                                                             , q = 0
                                                                                             , tauD = tauD_list
                                                                                             , D = D_list
                                                                                             , type = dplyr::if_else(loc_data$cp_flag=="P","put","call")
                                                                                             , M = 350
                                                                                             , uniroot.control = list(interval = c(1e-2,2.5))
                                                                                             )
                                          , impl_volatility_manual_offer = impl_vol_vectorized(exercise = "american"
                                                                                             , price = best_offer
                                                                                             , S = loc_data$close
                                                                                             , X = loc_data$strike_price/1000
                                                                                             , tau = loc_data$time_to_maturity
                                                                                             , r = loc_data$risk_free_rate
                                                                                             , q = 0
                                                                                             , tauD = tauD_list
                                                                                             , D = D_list
                                                                                             , type = dplyr::if_else(loc_data$cp_flag=="P","put","call")
                                                                                             , M = 350
                                                                                             , uniroot.control = list(interval = c(1e-2,2.5))
                                                                                             )
                )
              
              loc_data
            })
          
          
          save(out_data, file = sprintf("%s/%s-secid-%s-year-%s-month-%s.RData", save_path, save_filename, unique(out_data$secid), unique(out_data$year), unique(out_data$month)))
          
          out_data
        })
  
  option_and_iv_data 
}