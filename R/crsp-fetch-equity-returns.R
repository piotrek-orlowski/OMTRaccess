
#' CRSP stock data
#'
#' @param permnos 
#' @param frequency 
#' @param date_start 
#' @param date_end 
#' @param save_path 
#'
#' @return 
#' @export
#'
#' @examples
fetch_stock_data <- function(permnos
	, frequency = "daily"
	, date_start = NULL
	, date_end = NULL
	, save_path = NULL){
	
  # databse setup
  myWrdsAccess::wrds_connect()

  # stocks query
  stocks <- dplyr::tibble(permno = permnos)

  # CRSP table select
  crsp_table <- dplyr::if_else(frequency == "daily"
  	, "crspa.dsf"
  	, "crspa.msf")

  # SQL
  security_query_format <- "SELECT permno, date, bidlo, askhi, prc, vol, ret, bid, ask, shrout, cfacpr, cfacshr, openprc, retx
  	FROM %s as sf 
  	where sf.permno in (%s)"
  	
  date_append <- "\n and	date between '%s' and '%s'"
  
  if(!(is.null(date_start) & is.null(date_end))){
    security_query_format <- paste0(security_query_format, date_append)
  }

  name_query_format <- "SELECT permno
    , date
    , ticker
    , comnam
    , ncusip 
    from crspa.dse
     where permno in (%s) and event like 'NAMES'"

  # Adapt to multiple firms
  if(!(is.null(date_start) & is.null(date_end))){
   security_query_format <- sprintf(security_query_format
    , crsp_table, paste0(rep("%s",nrow(stocks))
    , collapse=",")
    , date_start
    , date_end) 
  } else {
    security_query_format <- sprintf(security_query_format
    , crsp_table, paste0(rep("%s",nrow(stocks))
    , collapse=",")) 
  }
  name_query_format <- sprintf(name_query_format, paste0(rep("%s",nrow(stocks)), collapse=","))

  security_query <- do.call(sprintf, args = c(list(security_query_format), as.list(stocks$permno)))
  name_query <- do.call(sprintf, args = c(list(name_query_format), as.list(stocks$permno)))

  security_data <- DBI::dbGetQuery(wrds, security_query)
  security_names <- DBI::dbGetQuery(wrds, name_query)

  security_data <- security_data %>% dplyr::inner_join(stocks)

  if(is.null(date_start) & is.null(date_end)){
    date_start <- min(security_data$date)
    date_end <- max(security_data$date)
  }

  loc_env <- environment()
  security_names <- security_names %>%
  	dplyr::mutate(date_end = dplyr::lead(date) - lubridate::days(1)) %>%
  	dplyr::rename(date_start = date) %>%
  	dplyr::mutate(date_end = dplyr::if_else(is.na(date_end), Sys.Date(), date_end)) %>%
  	dplyr::filter(date_end >= loc_env$date_start, date_start <= loc_env$date_end)

  myWrdsAccess::wrds_disconnect()
  
  save(security_data, security_names, file = save_path)

}