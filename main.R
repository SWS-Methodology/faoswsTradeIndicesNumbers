#' Main Script for the Trade Indices Numbers plugin
#' 
#' All the utilities functions are in Functions.R files
#' The main part of the calculation is done in Trade_indices_Script.R files
#' TIN calculations are split in 3Y chunks due to limited processing power



START <- Sys.time()

# Plugin settings --------------------------------------------------------------

plugin_name = "Trade Indices Numbers"
wd = "~/TradeIndicesNumber" # Rstudio Directory
env = "PROD" # QA, PROD or SWS
CHINA_FIX = T

# Status function --------------------------------------------------------------

status_message <-
  function(text = NULL,
           time = NULL,
           progress = NULL) {
    if (!is.null(text)) {
      if (text == "ok") {
        message(paste0(" - Done", "\n"))
      } else {
        message(paste(c(
          "Your", plugin_name, "Plugin", text
        ),
        collapse = " "))
        return(invisible(Sys.time()))
      }
      
    }
    
    if (!is.null(time)) {
      message(paste0(" - Done : ", format(Sys.time() - time), "\n"))
    }
    if (!is.null(progress) &
        !CheckDebug()) {
      system(paste("tm update", progress))
    }
  }

ok = "ok"

# Loading libraries ------------------------------------------------------------


status_message("has started.")

status_message("is loading libraries.")

if (env == "QA")
  .libPaths("/newhome/shared/R/3.3.3/lib")
if (env == "PROD")
  .libPaths("/newhome/shared/Library/3.3.3/")


suppressMessages({
  library(faosws)
  library(faoswsUtil)
  library(faoswsFlag)
  library(data.table)
  library(stringr)
  #library(dplyr)
  library(methods)
  library(tidyr)
})

# Setting the environment ------------------------------------------------------

if (CheckDebug()) {
  suppressWarnings({
    library(faoswsModules)
    SETT <- ReadSettings(paste(wd, "sws.yml", sep = "/"))
    SetClientFiles(SETT[["certdir"]])
    GetTestEnvironment(baseUrl = SETT[["server"]], token = SETT[["token"]])
    
    #   Load R functions
    invisible(lapply(
      list.files(
        paste0(wd, "/R functions"),
        full.names = T,
        pattern = ".R"
      ),
      source
    ))
  })
}

status_message(ok, progress = 5)

# Dataset chosen  --------------------------------------------------------------

domain_ = swsContext.datasets[[1]]@domain
dataset_ = swsContext.datasets[[1]]@dataset

# Parameters -------------------------------------------------------------------

time <- status_message("is reading its parameters.")


# Plugin parameters

base_year = as.numeric(ReadDatatable("ti_base_year")[[1, 1]])
base_period = as.character(c(base_year - 1,
                             base_year,
                             base_year + 1))

param_source <- swsContext.computationParams$source
param_aggr.country <- NULL # Deprecated
param_aggr.item <- NULL # Deprecated
param_output <- swsContext.computationParams$output
param_impexp <- swsContext.computationParams$importexport
param_year_filter <- swsContext.computationParams$year_range


# Selected query

selected_elem <- query_keys("measuredElementTrade") ; selected_elem = import_export("output") # filtered by import/export parameter
selected_years <- query_keys("timePointYears")
selected_items <- query_keys("measuredItemCPC")
selected_countries <- query_keys("geographicAreaM49")

# filtering year
if (!is.null(param_year_filter)) {
  if (param_year_filter == "") {
    param_year_filter = NULL
  } # FIX To avoid error when erasing the param year string
}

if (!is.null(param_year_filter)) {
  Y1 <- as.numeric(sub("^(.*?)-.*", "\\1", param_year_filter))
  Y2 <- as.numeric(sub(".*-(.*)", "\\1", param_year_filter))
  
  #expand the session according to year range filter
  selected_years <- as.character(seq(min(Y1, Y2), max(Y1, Y2)))
  
  #filter the selected years of the query according to the parameter
  # selected_years <-
  #     selected_years[selected_years %in% seq(min(Y1, Y2), max(Y1, Y2))]
  # 
  # 
  # if ( length(selected_years) == 0 ){
  #     stop(sprintf("'Year Range Filter' [%s] out of Session's timePointYears query [%s].",
  #                  param_year_filter,
  #                  paste(min(query_keys("timePointYears")),
  #                        max(query_keys("timePointYears")),
  #                        sep = "-")
  #                  ))
  # }  
}



status_message(time = time, progress = 10)

# Support Datatables

time <- status_message("is reading support datatables.")

# Item list, according to QCL domain in FAOSTAT

TI_item_list <- ReadDatatable("ti_item_list")
setnames(TI_item_list,
         old = "cpc_code",
         new = "measuredItemCPC")

# Multipliers for area not FOB/CIF, if the end_date is not specified in the
# parameter table, it will be applied up to the current date

TI_multipliers <- ReadDatatable(
  "ti_fob_cif_multipliers",
  columns = c("areacode_m49",
              "start_year",
              "end_year",
              "multiplier")
)
setnames(TI_multipliers,
         old = "areacode_m49",
         new = "geographicAreaM49")
TI_multipliers[is.na(end_year)]$end_year <-
  as.character(format(Sys.Date(),
                      format = "%Y"))

# Item group composition

TI_item_group <- ReadDatatable(
  "ti_aggregation_table",
  where = c("var_type = 'item'"),
  columns = c("var_group_code",
              "var_code",
              "var_element",
              "factor")
)

setnames(TI_item_group,
         old = "var_code",
         new = "measuredItemCPC")

factor_diff <- unique(TI_item_group$var_element)

TI_item_group[var_element == " ", var_element := "Default"]

TI_item_group <-
  dcast(TI_item_group,
        var_group_code + measuredItemCPC ~ var_element,
        value.var = "factor")

for (i in c('5610', '5910', '5622', '5922')) {
  if (i %in% factor_diff) {
    eval(parse(
      text =
        paste0("TI_item_group[is.na(`", i, "`), `", i, "` := Default]")
    ))
  }
}

# Countries list

TI_countries_list <- ReadDatatable("ti_country_list",
                                   columns = c("m49_code",
                                               "start_year",
                                               "end_year"))

TI_countries_list[, m49_code := sub("^0+", "", m49_code)]
TI_countries_list[is.na(start_year)]$start_year = "1900"
TI_countries_list[is.na(end_year)]$end_year = "9999"

setnames(TI_countries_list,
         old = "m49_code",
         new = "geographicAreaM49")

# Countries group composition

TI_countries_group <- ReadDatatable(
  "ti_aggregation_table",
  where = c("var_type = 'area' AND factor <> '0'"),
  columns = c("var_group_code",
              "var_code")
)
TI_countries_group[, var_group_code := sub("^0+", "", var_group_code)]
TI_countries_group[, var_code := sub("^0+", "", var_code)]

setnames(TI_countries_group,
         old = "var_code",
         new = "geographicAreaM49")

if (CHINA_FIX == T) {
  TI_countries_list$geographicAreaM49[TI_countries_list$geographicAreaM49 == "156"] = "1248"
  TI_countries_group$geographicAreaM49[TI_countries_group$geographicAreaM49 == "156" ] = "1248"
  
} # China Code Fix

# Conversion table for codes mapping patch

TI_convertion = ReadDatatable("ti_code_conversion",
                              columns = c("faostat_code",
                                          "sws_code"))

status_message(time = time, progress = 15)

# Patch items codes -----------------------------------------------------------

# 'Beet Pulp' item is saved under CPC code 39149.01 in FAOSTAT and code 39140.01 in SWS (' Beet Pulp ')
# 'Fine animal hair, n.e.c.' is saved under CPC code 02943.90 in FAOSTAT and code 02943.02 in SWS (' Fine hair, n.e. ')
# 'Juice of fruits n.e.c.' is saved under CPC code 21439.90 in FAOSTAT and code  in SWS (' Juice of fruits n.e. ')
# 'Rice, paddy (rice milled equivalent)' is saved under CPC code F0030 in FAOSTAT and code 23161.02 in SWS (' Rice, Milled ')

status_message("is correcting CPC codes.")

TI_item_list <- ReadDatatable("ti_item_list")
setnames(TI_item_list,
         old = "cpc_code",
         new = "measuredItemCPC")

for (i in 1:nrow(TI_convertion)) {
  TI_item_list <- switch_code(
    TI_item_list,
    'measuredItemCPC',
    as.character(TI_convertion[i, 1, with = F]),
    as.character(TI_convertion[i, 2, with = F])
  )
}

TI_item_list = TI_item_list[!duplicated(TI_item_list$measuredItemCPC)]

if (any(TI_convertion$sws_code %in% TI_item_group$measuredItemCPC)) {
  TI_item_group = TI_item_group[!measuredItemCPC == TI_convertion$faostat_code[TI_convertion$sws_code %in% TI_item_group$measuredItemCPC]]
}

for (i in 1:nrow(TI_convertion)) {
  TI_item_group <- switch_code(
    TI_item_group,
    'measuredItemCPC',
    as.character(TI_convertion[i, 1, with = F]),
    as.character(TI_convertion[i, 2, with = F])
  )
}

status_message(ok, progress = 20)


# Main Function, sliced due to memory shortage issues

domain_input = if (param_source == "disseminated") {
  "disseminated"
} else {
  "trade"
}

dataset_input = if (param_source == "disseminated") {
  "trade_crops_livestock_disseminated"
} else {
  "total_trade_cpc_m49"
}


# Save Base Period Data from SWS

bp <- status_message("is calculating base period values.")

data_BY <- Base_year_data(base_period = base_period)

#Check if all the selected years are inside the base period
BY_test <- all( selected_years %in% base_period )

#If all selected year are in Base Period, show log message
if(BY_test) { 
  ELEMENTS_BY <- Trade_indices(base_period, BP = T)
} else {
  ELEMENTS_BY <- suppressMessages(Trade_indices(base_period, BP = T))
}

PROGRESS = 25
status_message(time = bp, progress = PROGRESS)

selected_year_NO_BY <- setdiff(selected_years, base_period)

if(! BY_test ){
  
  ## Split calculations by 3 years subsets to reduce memory usage
  
  if (length(selected_years) >= 4) {
    sliced <-
      status_message("is splitting calculations in three-years groups for memory resources purposes. \n")
    
    sliced_years <-  year_slicer(selected_year_NO_BY)
    
    increment = round(50 / length(sliced_years))
    increment_for <- 100 / length(sliced_years)
    progress_for = 0
    
    ELEMENTS <- list()
    
    
    for (years in sliced_years) {
      min = min(years)
      max = max(years)
      range = paste(min, max, sep = " - ")
      
      
      message(sprintf("> CALCULATING YEARS [%s] :\n", range))
      
      ELEMENTS[[range]] <- Trade_indices(years)
      
      gc()
      
      progress_for = progress_for + increment_for
      message(sprintf("> [%s] Done - %s Complete.\n", range, paste0(round(
        progress_for, 2
      ), "%")))
      
      PROGRESS = PROGRESS + increment
      if (!CheckDebug()) {
        system(paste("tm update", PROGRESS))
      }
      
    }
    
    ELEMENTS <- rbindlist(ELEMENTS)
    
    message(sprintf("> Completed in %s \n",
                    format(round(
                      difftime(Sys.time(), sliced), 2
                    ))))
    
    
  } else {
    ELEMENTS <- Trade_indices(selected_years)
    
  }
  
  
  ELEMENTS <- rbind(ELEMENTS, ELEMENTS_BY)[timePointYears %in% selected_years]
  
} else {
  
  ELEMENTS <- ELEMENTS_BY[timePointYears %in% selected_years]
  
}

rm(list = c("ELEMENTS_BY", "data_BY"))
gc()


PROGRESS = 75
if (!CheckDebug()) {
  system(paste("tm update", PROGRESS))
}


# Save data ---------------------------------------------------------------

save_data <- ELEMENTS
rm(ELEMENTS)

# Not null elements

save_data <- save_data[is.finite(Value) & Value > 0]

# Rounding output 

save_data[, Value := round(Value, 2) ]

# Return queried rows

save_data <- save_data[timePointYears %in% selected_years]
save_data <-
  save_data[measuredElementTrade %in% selected_elem] # Selected Elements

if (!is.null(param_output)) {
  if (param_output == "query") {
    save_data <- save_data[geographicAreaM49 %in% selected_countries]
    save_data <- save_data[measuredItemCPC %in% selected_items]
  }
}

if (!is.null(param_aggr.country)) {
  if (param_aggr.country == "single") {
    save_data <-
      save_data[geographicAreaM49 %in% TI_countries_list$geographicAreaM49]
  }
  
  if (param_aggr.country == "aggr") {
    save_data <-
      save_data[geographicAreaM49 %in% unique(TI_countries_group$var_group_code)]
  }
}

if (!is.null(param_aggr.country)) {
  if (param_aggr.item == "single") {
    save_data <-
      save_data[measuredItemCPC %in% TI_item_list$measuredItemCPC]
  }
  
  if (param_aggr.item == "aggr") {
    save_data <-
      save_data[measuredItemCPC %in% unique(TI_item_group$var_group_code)]
  }
}

# Flags
save_data <-
  save_data[, c('flagObservationStatus', 'flagMethod') := .("E", "i")]

if (CHINA_FIX == T) {
  save_data$geographicAreaM49[save_data$geographicAreaM49 == "1248"] = "156"
}



# Metadata for single data point - removed for Block-metadata

# config <-
#     GetDatasetConfig(swsContext.datasets[[1]]@domain, swsContext.datasets[[1]]@dataset)
# metadata <- setDT(save_data[, mget(config$dimensions)])
#
# metadata[, `:=`(
#     Metadata = "GENERAL",
#     Metadata_Element = "COMMENT",
#     Metadata_Language = "en",
#     Metadata_Value = paste0("Base year: ", base_year)
# )]

status_message(ok, progress = 80)


# Save

savetime <- status_message("is saving data.")

PROGRESS = 80
len = (95 - 80) / length(selected_elem)

increment_for <- 100 / length(selected_elem)
progress_for = 0


save <- list()

for (i in selected_elem) {
  st <- Sys.time()
  
  save[[i]] <- SaveData(
    domain = domain_,
    dataset = dataset_,
    data = save_data[measuredElementTrade == i],
    #metadata = metadata,
    waitTimeout = 100000
  )
  
  st = format(round(difftime(Sys.time(), st), 2))
  
  progress_for = progress_for + increment_for
  pc = str_pad(paste0(round(progress_for, 2), "%"), 4, "left")
  
  PROGRESS = PROGRESS + len
  
  status_message(progress = PROGRESS)
  message(sprintf(" [%s] > Elem. %s Saved. - %s ",
                  pc, str_pad(i, 3, "left"), st))
  
}

status_message(time = savetime)

# Metadata ----------------------------------------------------------------

status_message("is adding metadata.")

SaveBlockMetadata(domain_, dataset_, blockMetadata = list(
  BlockMetadata(
    selection = swsContext.datasets[[1]]@dimensions,
    metadata = Metadata(
      code = "GENERAL",
      language = "en",
      elements = list(MetadataElement(
        code = "COMMENT" ,
        value =  paste0("Base year: ", base_year)
      ))
    )
  )
))

# Logs  -------------------------------------------------------------------

LOG_table <- "ti_log"
status_message("is writing logs.")

if (is.null(param_aggr.item)) {
  param_aggr.item = "-"
}
if (is.null(param_aggr.country)) {
  param_aggr.country = "-"
}

time_elapsed <- format(round(difftime(Sys.time(), START), 2))

line_writed <-
  sum(unlist(lapply(save, function(x)
    x$inserted)) , na.rm = T)
line_omitted <-
  sum(unlist(lapply(save, function(x)
    x$ignored)) , na.rm = T)
line_discarded <-
  sum(unlist(lapply(save, function(x)
    x$discarded)) , na.rm = T)

LOG = data.table(
  user_ = swsContext.userEmail,
  exec_date = Sys.Date(),
  param_base_year = base_year,
  years_range = if (min(selected_years) == max(selected_years)) {
    as.character(min(selected_years))
  } else {
    sprintf("%s - %s", min(selected_years), max(selected_years))
  },
  source = if (param_source == "disseminated")
    "Disseminated Datasets"
  else
    "Total Trade (CPC)",
  
  # param_item_aggr = if (param_aggr.item == "single")
  #     "Single items"
  # else if (param_aggr.item ==  "aggr")
  #     "Items Aggregates"
  # else
  #     "Both",
  # 
  # param_country_aggr = if (param_aggr.country == "single")
  #     "Single Countries"
  # else if (param_aggr.country ==  "aggr")
  #     "Countries Aggregates"
  # else
  #     "Both",
  
  param_impexp = if (!is.null(param_impexp)) {
    param_impexp
  } else {
    "-"
  },
  
  line_writed = line_writed,
  line_omitted = line_omitted,
  line_discarded = line_discarded,
  time_elapsed = time_elapsed
)

changeset <- Changeset(LOG_table)
AddInsertions(changeset, LOG)
Finalise(changeset)

status_message(ok, progress = 99)

paste0(
  sprintf(
    "Your Value of Agricultural Production Plugin is completed successfully! %s observations written, %s weren't updated, %s had problems, Total time elapsed : %s",
    line_writed,
    line_omitted,
    line_discarded,
    time_elapsed
  )
  
)
