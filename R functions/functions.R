### Functions ###


`%!in%` = Negate(`%in%`)

query_keys <-
  function(dimension)
    return(swsContext.datasets[[1]]@dimensions[[dimension]]@keys)

# Regions aggregate --------------------------------------------------------

regions_aggregate <- function(data_SWS, TI_countries_group) {
  atomic.merge <- function(grp, df1, df2, var) {
    Merge <- merge(df1,
                   df2[var_group_code == grp],
                   by = c("geographicAreaM49"),
                   #all.y = T,
                   allow.cartesian = TRUE)
    
    Merge <- Merge[, Value := sum(Value, na.rm = T),
                   by = c(
                     "var_group_code",
                     "measuredElementTrade",
                     "timePointYears",
                     "measuredItemCPC"
                   )]
    
    Merge[, geographicAreaM49 := var_group_code]
    Merge[, var_group_code := NULL]
    Merge <- unique(Merge)
    
    return(Merge)
  }
  
  TI_countries_group <-
    split(TI_countries_group, TI_countries_group$var_group_code)
  
  
  data_regions <- lapply(names(TI_countries_group),
                         function(x)
                           atomic.merge(x, data_SWS, TI_countries_group[[x]]))
  data_regions <- rbindlist(data_regions)
  
  return(data_regions)
  
}


# items aggregates --------------------------------------------------------

items_aggregate <- function(data_SWS, TI_item_group) {
  data_item.aggr <- merge(data_SWS[, .(
    measuredElementTrade,
    geographicAreaM49,
    measuredItemCPC,
    timePointYears,
    Value
  )],
  TI_item_group,
  by = "measuredItemCPC",
  allow.cartesian = TRUE)
  
  # Applying multipliers for aggregation
  
  for (i in c('5610', '5910', '5622', '5922')) {
    if (i %in% factor_diff) {
      data_item.aggr[measuredElementTrade == i , Value := Value * as.numeric(get(i))]
    }
  }
  
  data_item.aggr[measuredElementTrade %!in% factor_diff, Value := Value * as.numeric(Default)]
  
  data_item.aggr <-
    data_item.aggr[, list(Value = sum(Value, na.rm = T)),
                   by = c(
                     "measuredElementTrade",
                     "geographicAreaM49",
                     "timePointYears",
                     "var_group_code"
                   )]
  
  setnames(data_item.aggr, "var_group_code", "measuredItemCPC")
  
  return(data_item.aggr)
}


# Switch codes ------------------------------------------------------------

switch_code <- function(DT, COL, OLD, NEW) {
  dt <- as.data.frame(DT)
  dt[which(dt[, which(names(dt) == COL)] == OLD), which(names(dt) == COL)] = NEW
  dt <- as.data.table(dt)
  return(dt)
  
}


# Import Data From SWS  ----------------------------------------------------------

SWS_data <- function(domain,
                     dataset,
                     keys = NULL,
                     Flag = F) {
  dimension_name = GetDatasetConfig(domain, dataset)$dimensions
  ndim = length(dimension_name)
  
  if (is.null(keys) | length(keys) != ndim) {
    writeLines(c(
      "Provide keys in a list with the following order : ",
      " ",
      paste(1:ndim, dimension_name, sep = ". ")
    ))
    
    # error if not enough keys provided --------------------------------------------
    
    if (length(keys) != ndim &
        !is.null(keys)) {
      stop("Different number of dimentions")
    }
    
  } else {
    if (!is.list(keys)) {
      stop("keys parameter is not a list")
    }
    
    #-------------------------------------------------------------------------------
    
    dimensions = list()
    
    for (i in 1:ndim) {
      dimensions[[i]] <- Dimension(name = dimension_name[i],
                                   keys =  as.character(na.omit(unlist(keys[i]))))
    }
    
    DatasetKey <- DatasetKey(domain = domain,
                             dataset =  dataset,
                             dimensions = dimensions)
    
    if (Flag == T) {
      Data <-
        GetData(DatasetKey)
    } else {
      Data <- GetData(DatasetKey, flags = F)
    }
    
    return(Data)
  }
}



# Years Slicer ----------------------------------------------------------------

year_slicer <- function(years_vector) {
  length_group = 3
  
  # Calculate the number of complete groups of 3 elements
  num_complete_groups <-
    length(selected_year_NO_BY) %/% length_group
  
  # Calculate the number of remaining elements
  num_remaining_elements <-
    length(selected_year_NO_BY) %% length_group
  
  remaining_elements <-
    tail(selected_year_NO_BY, n = num_remaining_elements)
  
  
  # Split the vector into groups of 5 elements (excluding the last element if incomplete)
  sliced_years <-
    split(
      selected_year_NO_BY[selected_year_NO_BY %!in% remaining_elements] ,
      rep(
        1:num_complete_groups,
        each = length_group,
        length.out = length(selected_year_NO_BY) -
          num_remaining_elements
      )
    )
  
  # Add the remaining elements as a separate group if any
  if (num_remaining_elements > 0) {
    sliced_years <- c(sliced_years, list(remaining_elements))
  }
  
  return(sliced_years)
  
}




# Determine input elements required ---------------------------------------


import_export <- function(elem_type) {
  elem_import <- c("64",
                   "65",
                   "462",
                   "464",
                   "465")
  
  elem_export <- c("94",
                   "95",
                   "492",
                   "494",
                   "495")
  
  if (elem_type == "input") {
    import <- c()
    export <- c()
    
    if (any(selected_elem %in% elem_import)) {
      import <- c('5610', '5622')
    }
    if (any(selected_elem %in% elem_export)) {
      export <- c('5910', '5922')
    }
    
    # filter with import/export parameter to split calculations
    
    if (!is.null(param_impexp)) {
      if (param_impexp == "import") {
        import <- c('5610', '5622')
        export <- NULL
      }
      if (param_impexp == "export") {
        import = NULL
        export <- c('5910', '5922')
        
      }
    }
    key_elem = c(import, export)
    
    return(key_elem)
    
  } else if (elem_type == "output") {
    
    if (!is.null(param_impexp)) {
      if (param_impexp == "import") {
        
        selected_elem <- selected_elem[selected_elem %in% elem_import]
      }
      if (param_impexp == "export") {
        
        selected_elem <- selected_elem[selected_elem %in% elem_export]
      }
    }
    return(selected_elem)
    
  }
}

# Import Data  ------------------------------------------------------------


Base_year_data <- function(base_period) {
  key_item = TI_item_list$measuredItemCPC
  
  key_geo = TI_countries_list$geographicAreaM49
  
  key_elem = import_export("input")
  
  key_year =  base_period
  
  
  
  data_base_period = SWS_data(
    domain = domain_input,
    dataset = dataset_input,
    keys = list(key_geo, key_elem, key_item, key_year)
  )
  
  # Remove countries out of validity period
  
  data_base_period <- merge(data_base_period,
                            TI_countries_list,
                            by = "geographicAreaM49")
  
  data_base_period <-
    data_base_period[timePointYears <= end_year &
                       timePointYears >= start_year,]
  
  data_base_period <-
    data_base_period[, c('end_year', 'start_year') := NULL]
  
  return(data_base_period)
}
