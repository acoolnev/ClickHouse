set (CLICKHOUSE_LAMBDA_SOURCES LambdaConnection.cpp LambdaServer.cpp LambdaTable.cpp)

set (CLICKHOUSE_LAMBDA_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
        ch_contrib::aws_lambda_runtime
)

clickhouse_program_add(lambda)

target_link_libraries(clickhouse-lambda-lib PRIVATE clickhouse-server-lib)

if (TARGET ch_rust::skim)
    target_link_libraries(clickhouse-lambda-lib PRIVATE ch_rust::skim)
endif()
if (TARGET ch_contrib::azure_sdk)
    target_link_libraries(clickhouse-lambda-lib PRIVATE ch_contrib::azure_sdk)
endif()

# Always use internal readpassphrase
target_link_libraries(clickhouse-lambda-lib PRIVATE readpassphrase)

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
    set (WITH_COVERAGE ON)
    target_link_libraries(clickhouse-lambda-lib PRIVATE ${LIB_FUZZING_ENGINE})
endif ()
