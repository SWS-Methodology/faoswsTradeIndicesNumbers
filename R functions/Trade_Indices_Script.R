
# Main calculations -------------------------------------------------------


Trade_indices <- function (years_chunck, BP = F) {
    # Pull Data from SWS ----------------------------------------------------------
    
    time <- status_message("is pulling data from SWS.")
    
    
    if (BP == F) {
        key_item = TI_item_list$measuredItemCPC
        
        key_geo = TI_countries_list$geographicAreaM49
        
        key_elem = import_export("input")
        
        key_year = setdiff(years_chunck, base_period)
        
        
        
        data_SWS = SWS_data(
            domain = domain_input,
            dataset = dataset_input,
            keys = list(key_geo, key_elem, key_item, key_year)
        )
        
        # Remove countries out of validity period
        
        data_SWS <- merge(data_SWS,
                          TI_countries_list,
                          by = "geographicAreaM49")
        
        data_SWS <- data_SWS[timePointYears <= end_year &
                                 timePointYears >= start_year, ]
        
        data_SWS <- data_SWS[, c('end_year', 'start_year') := NULL]
        
        data_SWS <- rbind(data_SWS, data_BY)
        
        status_message(time = time)
        
    } else {
        data_SWS = data_BY
        
        status_message(ok)
    }
    
    
    # Add Regions / Special Regions -------------------------------------------
    
    time <- status_message("is calculating Regions aggregates.")
    
    # Aggregate by regions
    
    region_aggregate_plugin <-
        regions_aggregate(data_SWS, TI_countries_group)
    
    
    data_SWS <-
        rbind(data_SWS, region_aggregate_plugin)
    
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
    
    # Compare computed Regional aggregate with the one in the session
    
    region_aggregate_session <- SWS_data(
        domain_,
        dataset_,
        keys = list(
            K_geo = unique(region_aggregate_plugin$geographicAreaM49),
            K_elem = unique(region_aggregate_plugin$measuredElementTrade),
            K_item = unique(region_aggregate_plugin$measuredItemCPC),
            K_year = unique(region_aggregate_plugin$timePointYears)
        )
    )
    
    data_SWS <- merge(
        data_SWS,
        region_aggregate_session,
        by = c(
            "geographicAreaM49",
            "measuredElementTrade",
            "measuredItemCPC",
            "timePointYears"
        ),
        all.x = T
    )
    
    data_SWS <-
        data_SWS[is.na(Value.y), Value.y := Value.x][, Value.x := NULL]
    
    setnames(data_SWS, "Value.y", "Value")
    
    rm(list = c('region_aggregate_session', 'region_aggregate_plugin'))
    
    # Item aggregated  --------------------------------------------------------
    
    time = status_message("is calculating item aggregates.")
    
    item_aggr_plugin <- items_aggregate(data_SWS, TI_item_group)
    
    data_SWS <-   rbind(data_SWS, item_aggr_plugin)
    
    # Compare computed Item aggregate with the one in the session
    
    item_aggr_session <- SWS_data(
        domain_,
        dataset_,
        keys = list(
            K_geo = unique(item_aggr_plugin$geographicAreaM49),
            K_elem = unique(item_aggr_plugin$measuredElementTrade),
            K_item = unique(item_aggr_plugin$measuredItemCPC),
            K_year = unique(item_aggr_plugin$timePointYears)
        )
    )
    
    data_SWS <- merge(
        data_SWS,
        item_aggr_session,
        by = c(
            "geographicAreaM49",
            "measuredElementTrade",
            "measuredItemCPC",
            "timePointYears"
        ),
        all.x = T
    )
    
    data_SWS <-
        data_SWS[is.na(Value.y), Value.y := Value.x][, Value.x := NULL]
    
    setnames(data_SWS, "Value.y", "Value")
    
    rm(list = c('item_aggr_session', 'item_aggr_plugin'))
    
    status_message(time = time)
    
    # Calculations  -----------------------------------------------------------
    
    time <- status_message("is caluculating selected elements.")
    
    ELEMENTS <- list()
    
    ## Element 461 - 491 -------------------------------------------------------
    
    elem.461.491 <-
        setDT(data_SWS[measuredElementTrade %in% c("5610", "5910")])
    
    elem.461.491.avg <-
        setDT(elem.461.491[timePointYears %in% base_period, ])
    elem.461.491.avg <-
        elem.461.491.avg[, Mean := list(mean(Value, na.rm = T)),
                         by = c("measuredElementTrade",
                                "geographicAreaM49",
                                "measuredItemCPC")]
    elem.461.491.avg <-
        elem.461.491.avg[timePointYears == base_year, ][, c('Value', 'timePointYears') := NULL]
    
    elem.461.491 <- merge(
        elem.461.491,
        elem.461.491.avg,
        by = c(
            "measuredElementTrade",
            "geographicAreaM49",
            "measuredItemCPC"
        )
    )
    
    rm(elem.461.491.avg)
    elem.461.491[, Value := Value / Mean][, Mean := NULL]
    
    #if 461 in selected elements
    elem.461.491[measuredElementTrade == "5610"]$measuredElementTrade = "461"
    #message("   >  Elem. 461 - OK")
    
    #if
    elem.461.491[measuredElementTrade == "5910"]$measuredElementTrade = "491"
    #message("   >  Elem. 491 - OK")
    
    ELEMENTS[["461.491"]] <- elem.461.491
    rm(elem.461.491)
    
    
    
    ## Element 64 - 94  --------------------------------------------------------
    
    if (any(c("64", "94", "464", "494") %in% selected_elem)) {
        elem.64.94 <-
            setDT(data_SWS[measuredElementTrade %in% c("5622", "5922")])
        
        # Change elements code to match the merge keys
        elem.64.94[measuredElementTrade == "5622"]$measuredElementTrade = "461"
        elem.64.94[measuredElementTrade == "5922"]$measuredElementTrade = "491"
        
        elem.64.94 <- merge(
            elem.64.94,
            ELEMENTS$`461.491`,
            by = c(
                "measuredElementTrade",
                "geographicAreaM49",
                "measuredItemCPC",
                "timePointYears"
            )
        )
        
        elem.64.94[, Value := Value.x / Value.y][, c("Value.x", "Value.y") := NULL]
        
        elem.64.94[measuredElementTrade == "461"]$measuredElementTrade = "64"
        if ("64" %in% selected_elem )
            message("   >  Elem.  64 - OK")
        
        elem.64.94[measuredElementTrade == "491"]$measuredElementTrade = "94"
        if ("94" %in% selected_elem)
            message("   >  Elem.  94 - OK")
        
        ELEMENTS[["64.94"]] <- elem.64.94
        rm(elem.64.94)
        
    }
    
    
    ## Elements 65 - 95 --------------------------------------------------------
    
    if (any(c("65", "95", "465", "495") %in% selected_elem)) {
        elem.65.95 <-
            setDT(data_SWS[measuredElementTrade %in% c("5622", "5922") &
                               timePointYears %in% base_period])
        
        elem.65.95 <-
            elem.65.95[, Value := list(mean(Value, na.rm = T)),
                       by = c("measuredElementTrade",
                              "geographicAreaM49",
                              "measuredItemCPC")]
        
        elem.65.95 <-
            elem.65.95[timePointYears == base_year][, c('timePointYears') := NULL]
        
        elem.65.95[measuredElementTrade == "5622"]$measuredElementTrade = "461"
        elem.65.95[measuredElementTrade == "5922"]$measuredElementTrade = "491"
        
        elem.65.95 <- merge(
            ELEMENTS$`461.491`,
            elem.65.95,
            by = c(
                "measuredElementTrade",
                "geographicAreaM49",
                "measuredItemCPC"
            ),
            all.x = T
        )
        elem.65.95[, Value := Value.x * Value.y][, c("Value.x", "Value.y") := NULL]
        
        
        elem.65.95[measuredElementTrade == "461"]$measuredElementTrade = "65"
        if ("65" %in% selected_elem)
            message("   >  Elem.  65 - OK")
        
        elem.65.95[measuredElementTrade == "491"]$measuredElementTrade = "95"
        if ("95" %in% selected_elem)
            message("   >  Elem.  95 - OK")
        
        
        ELEMENTS[["65.95"]] <- elem.65.95
        rm(elem.65.95)
        
    }
    
    ## Element 462 - 492  ------------------------------------------------------
    
    if (any(c("462", "492") %in% selected_elem)) {
        elem.462.492 <-
            setDT(data_SWS[measuredElementTrade %in% c("5622", "5922")])
        
        elem.462.492.avg <-
            setDT(elem.462.492[timePointYears %in% base_period])
        elem.462.492.avg <-
            elem.462.492.avg[, Mean := list(mean(Value, na.rm = T)),
                             by = c("measuredElementTrade",
                                    "geographicAreaM49",
                                    "measuredItemCPC")]
        elem.462.492.avg <-
            elem.462.492.avg[timePointYears == base_year, ][, c('Value', 'timePointYears') := NULL]
        elem.462.492 <- merge(
            elem.462.492,
            elem.462.492.avg,
            by = c(
                "measuredElementTrade",
                "geographicAreaM49",
                "measuredItemCPC"
            )
        )
        rm(elem.462.492.avg)
        elem.462.492[, Value := (Value * 100) / Mean][, Mean := NULL]
        
        elem.462.492[measuredElementTrade == "5622"]$measuredElementTrade = "462"
        if ("462" %in% selected_elem)
            message("   >  Elem. 462 - OK")
        
        elem.462.492[measuredElementTrade == "5922"]$measuredElementTrade = "492"
        if ("492" %in% selected_elem)
            message("   >  Elem. 492 - OK")
        
        ELEMENTS[["462.492"]] <- elem.462.492
        rm(elem.462.492)
        
    }
    
    ## Element 464 - 494 -------------------------------------------------------
    
    if (any(c("464", "494") %in% selected_elem)) {
        elem.464.494 <- ELEMENTS$`64.94`[timePointYears %in% base_period, ]
        elem.464.494 <-
            elem.464.494[, Mean := list(mean(Value, na.rm = T)),
                         by = c("measuredElementTrade",
                                "geographicAreaM49",
                                "measuredItemCPC")]
        elem.464.494 <-
            elem.464.494[timePointYears == base_year, ][, c('Value', 'timePointYears') := NULL]
        elem.464.494 <- merge(
            ELEMENTS$`64.94`,
            elem.464.494,
            by = c(
                "measuredElementTrade",
                "geographicAreaM49",
                "measuredItemCPC"
            )
        )
        
        elem.464.494[, Value := (Value * 100) / Mean][, Mean := NULL]
        
        elem.464.494[measuredElementTrade == "64"]$measuredElementTrade = "464"
        if ("464" %in% selected_elem)
            message("   >  Elem. 464 - OK")
        
        elem.464.494[measuredElementTrade == "94"]$measuredElementTrade = "494"
        if ("494" %in% selected_elem)
            message("   >  Elem. 494 - OK")
        
        ELEMENTS[["464.494"]] <- elem.464.494
        rm(elem.464.494)
        
    }
    
    ## Element 465 - 495 -------------------------------------------------------
    
    if (any(c("465", "495") %in% selected_elem)) {
        elem.465.495 <- ELEMENTS$`65.95`[timePointYears %in% base_period, ]
        elem.465.495 <-
            elem.465.495[, Mean := list(mean(Value, na.rm = T)),
                         by = c("measuredElementTrade",
                                "geographicAreaM49",
                                "measuredItemCPC")]
        elem.465.495 <-
            elem.465.495[timePointYears == base_year, ][, c('Value', 'timePointYears') := NULL]
        elem.465.495 <- merge(
            ELEMENTS$`65.95`,
            elem.465.495,
            by = c(
                "measuredElementTrade",
                "geographicAreaM49",
                "measuredItemCPC"
            )
        )
        
        elem.465.495[, Value := (Value * 100) / Mean][, Mean := NULL]
        
        elem.465.495[measuredElementTrade == "65"]$measuredElementTrade = "465"
        if ("465" %in% selected_elem)
            message("   >  Elem. 465 - OK")
        
        elem.465.495[measuredElementTrade == "95"]$measuredElementTrade = "495"
        if ("495" %in% selected_elem)
            message("   >  Elem. 495 - OK")
        
        
        ELEMENTS[["465.495"]] <- elem.465.495
        rm(elem.465.495)
        
    }
    
    # Merge Results -----------------------------------------------------------
    
    ELEMENTS <- do.call("rbind", ELEMENTS)
    
    
    # Switching back to correct CPC code used for dissemination
    
    for (i in 1:nrow(TI_convertion)) {
        ELEMENTS <- switch_code(
            ELEMENTS,
            'measuredItemCPC',
            as.character(TI_convertion[i, 2, with = F]),
            as.character(TI_convertion[i, 1, with = F])
        )
    }
    
    status_message(time = time)
    
    if (BP == T) {
        return(ELEMENTS)
    } else {
        return(ELEMENTS[timePointYears %!in% base_period])
    }
    
}
