*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Produces traffic data work items.

Library             RPA.Tables
Library             RPA.Windows
Library             Collections
Resource            shared.robot


*** Variables ***
${TRAFFIC_JSON_FILE_PATH}       ${OUTPUT_DIR}${/}traffic.json
${TRAFFIC_DATA_URL}             https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json
${COUNTRY_COLUMN_KEY}           SpatialDim
${YEAR COLUMN KEY}              TimeDim
${ACCIDENT_RATE_COLUMN_KEY}     NumericValue
${GENDER_COLUMN_KEY}            Dim1


*** Tasks ***
Produce traffic data work items
    Download traffic data

    ${traffic_data}    Load traffic data as table
    ${filtered_data}    Filter and sort traffic data    ${traffic_data}
    ${latest_filtered_data}    Find latest traffic data for each country    ${filtered_data}
    ${traffic_data_ready_to_use}    Create work item payloads    ${latest_filtered_data}
    Save work item payloads    ${traffic_data_ready_to_use}


*** Keywords ***
Download traffic data
    Download
    ...    ${TRAFFIC_DATA_URL}
    ...    ${TRAFFIC_JSON_FILE_PATH}
    ...    overwrite=True

Load traffic data as table
    ${json}    Load JSON from file    ${TRAFFIC_JSON_FILE_PATH}
    ${table}    Create Table    ${json}[value]
    RETURN    ${table}

Filter and sort traffic data
    [Arguments]    ${data}
    #Variables used in this Keyword
    ${both_genders}    Set Variable    BTSX
    ${max_accident_rate}    Set Variable    ${5.0}

    Filter Table By Column    ${data}    ${GENDER_COLUMN_KEY}    ==    ${both_genders}
    Filter Table By Column    ${data}    ${ACCIDENT_RATE_COLUMN_KEY}    <    ${max_accident_rate}
    Sort Table By Column    ${data}    ${YEAR COLUMN KEY}
    RETURN    ${data}

Find latest traffic data for each country
    [Arguments]    ${table}
    ${table}    Group Table By Column    ${table}    ${COUNTRY_COLUMN_KEY}
    ${each_country_latest_data}    Create List
    FOR    ${group}    IN    @{table}
        ${first_row}    Pop Table Row    ${group}
        Append To List    ${each_country_latest_data}    ${first_row}
    END
    RETURN    ${each_country_latest_data}

Create work item payloads
    [Arguments]    ${table_list}
    ${work_item_payloads}    Create List
    FOR    ${row}    IN    @{table_list}
        ${item_payload}    Create Dictionary
        ...    country=${row}[${COUNTRY_COLUMN_KEY}]
        ...    year=${row}[${YEAR COLUMN KEY}]
        ...    rate=${row}[${ACCIDENT_RATE_COLUMN_KEY}]
        Append To List    ${work_item_payloads}    ${item_payload}
    END
    RETURN    ${work_item_payloads}

Save work item payloads
    [Arguments]    ${payloads}
    FOR    ${payload}    IN    @{payloads}
        Save work item payload    ${payload}
    END

Save work item payload
    [Arguments]    ${payload}
    ${variables}    Create Dictionary    ${WORK_ITEM_NAME}=${payload}
    Create Output Work Item    variables=${variables}    save=True
