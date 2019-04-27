#' Zero curve interpolation for option panel
#'
#' @param option_data 
#' @param zero_coupon_data 
#'
#' @return
#' @export
#'
#' @examples
interpolate_zero_curve_for_options <- function(option_data, zero_coupon_data){
  
  # take distinct dates and maturities Fri Oct 12 13:43:05 2018 ------------------------------
  option_dates_and_maturities <- option_data %>% 
    dplyr::select(date, exdate) %>% 
    dplyr::distinct()
  
  option_dates_and_maturities <- option_dates_and_maturities %>% 
    dplyr::group_by(date) %>% 
    dplyr::do({
      loc_data <- .
      
      # calculate days to maturity Fri Oct 12 13:45:42 2018 ------------------------------
      loc_data <- dplyr::mutate(loc_data, days_to_maturity = as.numeric(exdate-date))
      loc_data <- dplyr::arrange(loc_data, days_to_maturity)
      
      # get zc data for the day Fri Oct 12 13:46:51 2018 ------------------------------
      loc_zero_coupon <- dplyr::inner_join(zero_coupon_data, dplyr::distinct(dplyr::select(loc_data,date)), by = "date")
      # it might be the case there is no data for a given day Fri Oct 12 13:55:18 2018 ------------------------------
      # use the nearest previous zero curve
      if(nrow(loc_zero_coupon) == 0){
        loc_zero_coupon <- dplyr::filter(zero_coupon_data, date <= unique(loc_data$date))
        loc_zero_coupon <- dplyr::filter(loc_zero_coupon, date == max(date))
      }
      
      
      # interpolate Fri Oct 12 13:48:07 2018 ------------------------------
      loc_data <- dplyr::mutate(loc_data, risk_free_rate = approx(loc_zero_coupon$days, loc_zero_coupon$rate, loc_data$days_to_maturity, rule = 2)$y)
      
      loc_data
    }) %>% 
    dplyr::ungroup()
  
  option_dates_and_maturities <- option_dates_and_maturities %>% 
    dplyr::mutate(time_to_maturity = days_to_maturity/365)
  
  return(option_dates_and_maturities)
}

