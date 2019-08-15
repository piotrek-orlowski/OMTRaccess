#' Title extract equity option data with associated distributions and equity prices
#'
#' @param securities data.frame with columns secid and (optional) permno, and start_date, end_date
#' @param save_path 
#'
#' @return
#' @export
#'
#' @examples
fetch_equity_option_data <- function(securities, save_path){

  # database setup  ---  2018-05-04 15:36:12  -----
  
  myWrdsAccess::wrds_connect() 

  # Modify seq query to search in CRSP if permnos provided Wed Aug 14 09:13:53 2019 ------------------------------
  # securities query  ---  2018-05-04 15:36:18  -----
  
  # securities <- data.frame(secid = secids)
  
  if("permno" %in% colnames(securities)){
    security_query_format <- "SELECT 
      permno
      , date
      , cfacpr
      , prc
      , ret
      /*, retx */
      , bid
      , ask
      , vol
      , shrout
      from crspa.dsf where permno in (%s) and date between '%s' and '%s'"
  } else {
    security_query_format <- "SELECT 
      secid,
      date,
      close,
      return,
      cfadj,
    /* cfret, */
      shrout
      , volume
      FROM
      optionm.secprd
      WHERE
      secid in (%s) and date between '%s' and '%s'"
  }
  
  security_query_format <- sprintf(security_query_format, paste0(rep("%s",nrow(securities)), collapse=","), min(securities$start_date), max(securities$end_date))
  
  if("permno" %in% colnames(securities)){
    security_query <- do.call(sprintf, args = c(list(security_query_format), as.list(securities$permno))) 
  } else {
    security_query <- do.call(sprintf, args = c(list(security_query_format), as.list(securities$secid))) 
  }
  
  security_data <- DBI::dbGetQuery(wrds, security_query)

  security_data <- security_data %>% dplyr::inner_join(securities)
  
  security_data <- security_data %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(date = as.Date(date))
  
  security_data <- security_data %>% 
    group_by(permno, secid) %>% 
    filter(between(date, unique(start_date), unique(end_date))) %>% 
    select(-start_date, -end_date) %>% 
    ungroup()
  
  # bring crsp-based table to same colnames as omtr-based table Wed Aug 14 11:32:55 2019 ------------------------------
  if("permno" %in% colnames(securities)){
    security_data <- security_data %>% 
      rename(volume = vol
             , close = prc
             , return = ret
             , cfadj = cfacpr
             )
  }
  
  
  # distributions query ---- Thu Oct 04 17:46:24 2018 ----
  
  if("permno" %in% colnames(securities)){
    distributions_query_format <- "SELECT 
    permno
    , date
    , facpr
    , divamt
    , paydt
    from crspa.dse
    WHERE
      permno in (%s) and date between '%s' and '%s' and event like 'DIST'" 
  } else {
    distributions_query_format <- "SELECT 
      secid,
      ex_date,
      adj_factor,
      amount,
      /* distr_type, */
      /* link_secid, */
      payment_date
      FROM
      optionm.distrd
      WHERE
      secid in (%s) and ex_date between '%s' and '%s'" 
  }
  
  distributions_query_format <- sprintf(distributions_query_format, paste0(rep("%s",nrow(securities)), collapse=","), min(securities$start_date), max(securities$end_date))
  
  if("permno" %in% colnames(securities)){
    distributions_query <- do.call(sprintf, args = c(list(distributions_query_format), as.list(securities$permno)))
  } else {
    distributions_query <- do.call(sprintf, args = c(list(distributions_query_format), as.list(securities$secid)))  
  }
  
  distributions_data <- DBI::dbGetQuery(wrds, distributions_query)
  
  distributions_data <- distributions_data %>% dplyr::inner_join(securities)
  
  distributions_data <- distributions_data %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate_at(.vars = colnames(.)[grepl("date|dt",colnames(.))], .funs = ~ as.Date(.))

  if("permno" %in% colnames(securities)){
    distributions_data <- distributions_data %>% 
      group_by(permno, secid) %>% 
      filter(between(date, unique(start_date), unique(end_date))) %>% 
      select(-start_date, -end_date)
  } else {
    distributions_data <- distributions_data %>% 
      group_by(permno, secid) %>% 
      filter(between(ex_date, unique(start_date), unique(end_date))) %>% 
      select(-start_date, -end_date)
  } 
  
  # Harmonize colnames as above for security_data Wed Aug 14 11:41:32 2019 ------------------------------
  if("permno" %in% colnames(securities)){
    distributions_data <- distributions_data %>% 
      rename(ex_date = date
             , adj_factor = facpr
             , amount = divamt
             , payment_date = paydt) %>% 
      mutate(adj_factor = adj_factor + 1.0)
  }
  
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
  volume, 
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
  AND
  date between '%s' and '%s'
  AND ss_flag LIKE '0'
  AND best_bid > 0
  ORDER BY secid , date , cp_flag DESC , strike_price;"
  
  option_data <- lapply(option_table_names$table_name, function(opt_tbl){
    option_query_table <- sprintf(option_query_format, opt_tbl, paste0(rep("%s",nrow(securities)), collapse=","), min(securities$start_date), max(securities$end_date))
    option_query <- do.call(sprintf, args = c(list(option_query_table), as.list(securities$secid)))
    option_data <- DBI::dbGetQuery(conn = wrds, statement = option_query)
    option_data
  })
  
  option_data <- dplyr::bind_rows(option_data)
  
  # option_data <- pool::dbGetQuery(conn = conn_pool, statement = option_query)
  
  option_data <- securities %>% dplyr::inner_join(option_data)
  
  option_data <- option_data %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(date = as.Date(date)) %>% 
    dplyr::mutate(exdate = as.Date(exdate))
  
  option_data <- option_data %>% 
    group_by(permno, secid) %>% 
    filter(between(date, unique(start_date), unique(end_date))) %>% 
    select(-start_date, -end_date)
    
  # get security tickers
  stock_names <- DBI::dbGetQuery(wrds, sprintf("select * from optionm.secnmd where secid in (%s)",paste0(securities$secid,collapse = ",")))
  
  stock_names <- stock_names %>% mutate(effect_date = as.Date(effect_date)) %>% group_by(secid) %>% filter(!(effect_date > max(securities$end_date)))
  
  stock_names <- stock_names %>%
    select(secid, effect_date, cusip, ticker) %>% 
    group_by(secid) %>% 
    arrange(effect_date) %>% 
    mutate(start_date = effect_date, end_date = lead(effect_date) - lubridate::days(1)) %>% 
    ungroup() %>% 
    select(-effect_date) %>% 
    mutate(end_date = if_else(is.na(end_date), max(securities$end_date), end_date)) %>% 
    mutate(start_date = pmax(start_date, min(securities$start_date))) %>% 
    arrange(secid, start_date) %>% 
    filter(!grepl("ZZZZ", ticker))
  
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
 
 zero_curve <- DBI::dbGetQuery(wrds, "select * from optionm.zerocd")
 
 myWrdsAccess::wrds_disconnect()
 
 zero_curve <- zero_curve %>% 
   dplyr::mutate(date = as.Date(date,format="%Y-%m-%d")) %>% 
   dplyr::mutate(rate = rate/100)
 
 zero_curve
}