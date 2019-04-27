#' Matching of options and distributions, pv of distributions
#'
#' @param option_data 
#' @param distributions_data
#' @param zero_coupon_data 
#'
#' @return
#' @export
#'
#' @examples
match_distributions_with_options <- function(option_data, distributions_data, zero_coupon_data){
  
  # take company id, date, exdate Fri Oct 12 14:16:55 2018 ------------------------------
  option_dates_and_maturities <- option_data %>% 
    dplyr::select(secid, date, exdate) %>% 
    dplyr::distinct()
  
  # join options expiries with distributions, fuzzy join required Fri Oct 12 14:17:27 2018 ------------------------------
  option_dates_and_maturities <- option_dates_and_maturities %>% 
    dplyr::group_by(secid, date) %>% 
    dplyr::do({
      loc_data <- .
      loc_data <- dplyr::arrange(loc_data,exdate)
      
      # pick out distributions within the expiry window for the company Fri Oct 12 14:40:48 2018 ------------------------------
      loc_distributions_data <- dplyr::filter(distributions_data, ex_date >= unique(loc_data$date), ex_date <= max(loc_data$exdate), secid == unique(loc_data$secid))
      loc_distributions_data <- dplyr::arrange(loc_distributions_data, ex_date)
      
      # get risk-free rates and interpolate for distribution pricing Fri Oct 12 14:51:36 2018 ------------------------------
      loc_zero_coupon <- dplyr::inner_join(zero_coupon_data, dplyr::distinct(dplyr::select(loc_data,date)), by = "date")
      # it might be the case there is no data for a given day Fri Oct 12 13:55:18 2018 ------------------------------
      # use the nearest previous zero curve
      if(nrow(loc_zero_coupon) == 0){
        loc_zero_coupon <- dplyr::filter(zero_coupon_data, date <= unique(loc_data$date))
        loc_zero_coupon <- dplyr::filter(loc_zero_coupon, date == max(date))
      }
      
      loc_distributions_data <- loc_distributions_data %>% dplyr::mutate(date = unique(loc_data$date))
      loc_distributions_data <- loc_distributions_data %>% 
        dplyr::mutate(risk_free_rate = approx(loc_zero_coupon$days, loc_zero_coupon$rate, as.numeric(ex_date-date), rule = 2)$y
               , time_to_distribution = as.numeric(ex_date-date)/365) %>% 
        dplyr::mutate(pv_of_dividend_amount = exp(-risk_free_rate * time_to_distribution) * amount)
      
      loc_distributions_data <- dplyr::select(loc_distributions_data,ex_date,amount,time_to_distribution,risk_free_rate,pv_of_dividend_amount,risk_free_rate)
      
      # join local data and distributions Fri Oct 12 14:41:19 2018 ------------------------------
      loc_data <- fuzzyjoin::fuzzy_left_join(loc_data, loc_distributions_data, by = c("exdate"="ex_date"), function(x,y) x>=y)
      
      
      # process Fri Oct 12 14:41:33 2018 ------------------------------
      loc_data <- loc_data %>% 
        dplyr::rename(dividend_amount = amount, dividend_ex_date = ex_date)
      
      loc_data
    }) %>% 
    dplyr::ungroup()
  
  return(option_dates_and_maturities)
}