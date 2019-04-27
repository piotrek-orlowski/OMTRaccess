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
  
  loc_env <- environment()
  parallel::clusterExport(cl, c("option_distributions", "option_risk_free_rates", "security_data", "save_path", "save_filename"), envir = loc_env)
  
  option_data <- option_data %>% 
    dplyr::mutate(year = lubridate::year(date), month = lubridate::month(date))
  
  option_data <- multidplyr::partition(option_data %>% dplyr::ungroup(), year, month, secid, cluster = cl)
  
  option_and_iv_data <- option_data %>% 
            dplyr::do({
              out_data <- dplyr::do(dplyr::group_by(.,date, strike_price, exdate, cp_flag),{
                loc_data <- .
                # join with distributions Tue Oct 16 15:03:51 2018 ------------------------------
                loc_distr_data <- dplyr::left_join(loc_data %>% dplyr::select(secid, date, exdate), option_distributions)
                # join with risk-free rates Tue Oct 16 15:06:04 2018 ------------------------------
                loc_data <- dplyr::left_join(loc_data, option_risk_free_rates)
                # join with price data Tue Oct 16 15:08:01 2018 ------------------------------
                loc_data <- dplyr::left_join(loc_data, security_data %>% dplyr::select(secid,date,close))
                # locate NAs in distribution (there might be NAs if no dividends before maturity) Tue Oct 16 16:52:16 2018 ------------------------------
                loc_distr_data <- dplyr::mutate_if(loc_distr_data, .predicate = is.numeric, .funs = dplyr::funs(dplyr::if_else(is.na(.),0,.)))
                # calculate IV Tue Oct 16 15:53:22 2018 ------------------------------
                loc_data <- dplyr::mutate(loc_data,impl_volatility_manual = tryCatch(NMOF::vanillaOptionImpliedVol(exercise = "american"
                                                                                                                       , price = 0.5*(best_bid+best_offer)
                                                                                                                       , S = loc_data$close
                                                                                                                       , X = loc_data$strike_price/1000
                                                                                                                       , tau = loc_data$time_to_maturity
                                                                                                                       , r = loc_data$risk_free_rate
                                                                                                                       , q = 0
                                                                                                                       , tauD = loc_distr_data$time_to_distribution
                                                                                                                       , D = loc_distr_data$dividend_amount
                                                                                                                       , type = dplyr::if_else(loc_data$cp_flag=="P","put","call")
                                                                                                                       , M = 350
                                                                                                                       , uniroot.control = list(interval = c(1e-2,4))
                ), error = function(e) NA_real_))
                # calculate bid/ask true IV Tue Oct 16 16:07:40 2018 ------------------------------
                loc_data <- dplyr::mutate(loc_data,impl_volatility_manual_bid = tryCatch(NMOF::vanillaOptionImpliedVol(exercise = "american"
                                                                                                                           , price = best_bid
                                                                                                                           , uniroot.control = list(interval = c(1e-2,2))
                                                                                                                           , S = loc_data$close
                                                                                                                           , X = loc_data$strike_price/1000
                                                                                                                           , tau = loc_data$time_to_maturity
                                                                                                                           , r = loc_data$risk_free_rate
                                                                                                                           , q = 0
                                                                                                                           , tauD = loc_distr_data$time_to_distribution
                                                                                                                           , D = loc_distr_data$dividend_amount
                                                                                                                           , type = dplyr::if_else(loc_data$cp_flag=="P","put","call")
                                                                                                                           , M = 250
                ), error = function(e) NA_real_)
                , impl_volatility_manual_offer = tryCatch(NMOF::vanillaOptionImpliedVol(exercise = "american"
                                                                                        , price = best_offer
                                                                                        , uniroot.control = list(interval = c(1e-2,2))
                                                                                        , S = loc_data$close
                                                                                        , X = loc_data$strike_price/1000
                                                                                        , tau = loc_data$time_to_maturity
                                                                                        , r = loc_data$risk_free_rate
                                                                                        , q = 0
                                                                                        , tauD = loc_distr_data$time_to_distribution
                                                                                        , D = loc_distr_data$dividend_amount
                                                                                        , type = dplyr::if_else(loc_data$cp_flag=="P","put","call")
                                                                                        , M = 250
                ), error = function(e) NA_real_))
              
              loc_data
            })
          
          
          save(out_data, file = sprintf("%s/%s-secid-%s-year-%s-month-%s.RData", save_path, save_filename, unique(out_data$secid), unique(out_data$year), unique(out_data$month)))
          
          out_data
        })
  
  option_and_iv_data 
}