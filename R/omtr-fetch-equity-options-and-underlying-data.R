#' Title extract equity option data with associated distributions and equity prices
#'
#' @param secids 
#' @param save_path 
#'
#' @return
#' @export
#'
#' @examples
fetch_equity_option_data <- function(secids, save_path){

    # database setup  ---  2018-05-04 15:36:12  -----
  
  myWrdsAccess::wrds_connect() 

  # securities query  ---  2018-05-04 15:36:18  -----
  
  securities <- data.frame(secid = secids)
  
  # security_query_format <- "SELECT 
  #                             security_id,
  #                             date,
  #                             close_price,
  #                             total_return,
  #                             adjustment_factor,
  #                             adjustment_factor_2
  #                           FROM
  #                             ivydb_201604.security_price
  #                           WHERE
  #                             security_id in (%s)"
  
  security_query_format <- "SELECT 
  secid,
  date,
  close,
  return,
  cfadj,
  cfret,
  shrout
  FROM
  optionm.secprd
  WHERE
  secid in (%s)"
  
  security_query_format <- sprintf(security_query_format, paste0(rep("%s",nrow(securities)), collapse=","))
  
  security_query <- do.call(sprintf, args = c(list(security_query_format), as.list(securities$secid)))
  
  security_data <- dbGetQuery(wrds, security_query)

  security_data <- security_data %>% dplyr::inner_join(securities)
  
  security_data <- security_data %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(date = as.Date(date))
  
  # distributions query ---- Thu Oct 04 17:46:24 2018 ----
  
  distributions_query_format <- "SELECT 
  secid,
  ex_date,
  adj_factor,
  amount,
  distr_type,
  link_secid,
  payment_date
  FROM
  optionm.distrd
  WHERE
  secid in (%s)"
  
  distributions_query_format <- sprintf(distributions_query_format, paste0(rep("%s",nrow(securities)), collapse=","))
  
  distributions_query <- do.call(sprintf, args = c(list(distributions_query_format), as.list(securities$secid)))
  
  distributions_data <- dbGetQuery(wrds, distributions_query)
  
  distributions_data <- distributions_data %>% dplyr::inner_join(securities)
  
  distributions_data <- distributions_data %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate_at(.vars = colnames(.)[grepl("date",colnames(.))], .funs = dplyr::funs(as.Date(.)))

  # option data ---- Thu Oct 04 18:00:41 2018 ----
  
  option_table_names <- myWrdsAccess::get_wrds_table_names(db_name = "optionm")
  option_table_names <- option_table_names %>% dplyr::filter(grepl("opprcd", table_name))
  
  option_query_format <- "SELECT 
  secid,
  symbol,
  symbol_flag,
  optionid,
  date,
  strike_price,
  exdate,
  cp_flag,
  best_bid,
  best_offer,
  impl_volatility,
  delta,
  gamma,
  vega,
  theta,
  cfadj,
  ss_flag
  FROM
  OPTIONM.%s
  WHERE
  secid in (%s)
  AND 
  volume > 0
  ORDER BY secid , date , cp_flag DESC , strike_price;"
  
  option_data <- lapply(option_table_names$table_name, function(opt_tbl){
    option_query_table <- sprintf(option_query_format, opt_tbl, paste0(rep("%s",nrow(securities)), collapse=","))
    option_query <- do.call(sprintf, args = c(list(option_query_table), as.list(securities$secid)))
    option_data <- dbGetQuery(conn = wrds, statement = option_query)
    option_data
  })
  
  option_data <- dplyr::bind_rows(option_data)
  
  # option_data <- pool::dbGetQuery(conn = conn_pool, statement = option_query)
  
  option_data <- securities %>% dplyr::inner_join(option_data)
  
  option_data <- option_data %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(date = as.Date(date)) %>% 
    dplyr::mutate(exdate = as.Date(exdate))
    
  # get security tickers
  stock_names <- dbGetQuery(wrds, sprintf("select * from optionm.secnmd where secid in (%s)",paste0(securities$secid,collapse = ",")))
  
  myWrdsAccess::wrds_disconnect()
  
  save(option_data, distributions_data, security_data, stock_names, file = save_path)
}

#' Fetch zero curve
#'
#' @return
#' @export
#'
#' @examples
fetch_zero_coupon_curve <- function(){
 wrds <- myWrdsAccess::wrds_connect() 
 
 zero_curve <- dbGetQuery(wrds, "select * from optionm.zerocd")
 
 myWrdsAccess::wrds_disconnect()
 
 zero_curve <- zero_curve %>% 
   dplyr::mutate(date = as.Date(date,format="%Y-%m-%d")) %>% 
   dplyr::mutate(rate = rate/100)
 
 zero_curve
}