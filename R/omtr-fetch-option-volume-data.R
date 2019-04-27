#' Title extract option volume data
#'
#' @param secids 
#' @param save_path 
#'
#' @return
#' @export
#'
#' @examples
fetch_option_volume_data <- function(secids, save_path){
  
  # database setup  ---  2018-05-04 15:36:12  -----
  
  myWrdsAccess::wrds_connect()
  dbs <- myWrdsAccess::get_wrds_table_names(db_name = "optionm")
  
  # securities query  ---  2018-05-04 15:36:18  -----
  
  securities <- data.frame(secid = secids)
  
  volume_query_format <- "SELECT 
  *
  FROM
  optionm.opvold
  WHERE
  secid in (%s)"
  
  volume_query_format <- sprintf(volume_query_format, paste0(rep("%s",nrow(securities)), collapse=","))
  
  volume_query <- do.call(sprintf, args = c(list(volume_query_format), as.list(securities$secid)))
  
  volume_data <- RPostgres::dbGetQuery(wrds, volume_query)
  
  volume_data <- volume_data %>% dplyr::inner_join(securities)
  
  volume_data <- volume_data %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(date = as.Date(date))
  
  myWrdsAccess::wrds_disconnect()
  
  save(volume_data, file = save_path)
}
