# Aggregation Module

START <- Sys.time()

# Plugin settings --------------------------------------------------------------

plugin_name = "Trade Aggregates Module"
wd = "~/TradeIndicesNumber" # Rstudio Directory
env = "PROD" # QA, PROD or SWS
CHINA_FIX = T

# Status function --------------------------------------------------------------

status_message <-
    function(text = NULL, time = NULL) {
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
})

# Setting the environment ------------------------------------------------------

if (CheckDebug()) {
    suppressWarnings({
        library(faoswsModules)
        SETT <-
            ReadSettings(paste(wd, "swsAggregates.yml", sep = "/"))
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

status_message(ok)

# Dataset chosen  --------------------------------------------------------------

domain_ = swsContext.datasets[[1]]@domain
dataset_ = swsContext.datasets[[1]]@dataset

# Parameters -------------------------------------------------------------------

time <- status_message("is reading its parameters.")


# Selected query

selected_elem <- query_keys("measuredElementTrade")
selected_years <- query_keys("timePointYears")
selected_items <- query_keys("measuredItemCPC")
selected_countries <- query_keys("geographicAreaM49")

# Plugin parameters

param_source <- swsContext.computationParams$source
if( is.null(param_source) ){param_source = "disseminated" }
param_aggr <- swsContext.computationParams$aggregation #Parameter


status_message(time = time)

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

status_message(time = time)

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

status_message(ok)

# Pull Data from SWS ----------------------------------------------------------

time <- status_message("is pulling data from SWS.")


key_item = TI_item_list$measuredItemCPC

key_geo = TI_countries_list$geographicAreaM49

key_elem = c('5610', '5622', '5910', '5922')

key_year = selected_years

domain_input = switch (param_source,
    "disseminated" = "disseminated",
    "trade" = "trade")
dataset_input = switch (param_source,
                       "disseminated" = "trade_crops_livestock_disseminated",
                       "trade" = "total_trade_cpc_m49")





data_SWS = SWS_data(
    domain = "trade",
    dataset = "total_trade_cpc_m49",
    keys = list(key_geo, key_elem, key_item, key_year)
)



# Remove countries out of validity period

data_SWS <- merge(data_SWS,
                  TI_countries_list,
                  by = "geographicAreaM49")

data_SWS <- data_SWS[timePointYears <= end_year &
                         timePointYears >= start_year,]

data_SWS <- data_SWS[, c('end_year', 'start_year') := NULL]

status_message(time = time)

# Add Regions / Special Regions -------------------------------------------

time <- status_message("is calculating Regions aggregates.")

# Aggregate by regions

data_regions <- regions_aggregate(data_SWS, TI_countries_group)

data_SWS <-
    rbind(data_SWS, data_regions)

status_message(time = time)

# Area not FOB/CIF multiplier ---------------------------------------------

status_message("is applying Area not FOB/CIF multiplier.")

data_SWS <- merge(data_SWS,
                  TI_multipliers,
                  by = "geographicAreaM49",
                  all.x = T)

data_SWS[timePointYears >= start_year &
             timePointYears <= end_year &
             measuredElementTrade == "5622",
         Value := Value * as.numeric(multiplier)]

data_SWS[, c("start_year", "end_year", "multiplier") := NULL]

status_message(ok)

# Item aggregated  --------------------------------------------------------

time = status_message("is calculating item aggregates.")

data_item_aggr <- items_aggregate(data_SWS, TI_item_group)

status_message(time = time)


# Save Data ---------------------------------------------------------------

if (param_aggr == 'region')
    save_data <- setDT(data_regions)
if (param_aggr == 'item')
    save_data <- setDT(data_item_aggr)
if (param_aggr == 'both')
    save_data <- setDT(rbind(data_regions, data_item_aggr))



save_data <-
    save_data[, c('flagObservationStatus', 'flagMethod') := .("E", "i")]

if(CHINA_FIX == T) {save_data$geographicAreaM49[save_data$geographicAreaM49 == "1248"] = "156" }


savetime <- status_message("is saving data.")



save <- SaveData(
    domain = domain_,
    dataset = dataset_,
    data = save_data,
    #metadata = metadata,
    waitTimeout = 100000
)

status_message(time = savetime)


# Logs  -------------------------------------------------------------------

status_message("is writing logs.")




# End ---------------------------------------------------------------------

status_message("ended successfully.")
message(paste0(" - Total elapsed time : ", format(Sys.time() - START)))
