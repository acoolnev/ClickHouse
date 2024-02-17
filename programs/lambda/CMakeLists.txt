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


# Temporary solution to build the docker image. It should be moved to a shell script later.

set (CLICKHOUSE_LAMBDA_CONTAINER_SUPPLEMENTS 
    bootstrap
    Dockerfile
)

set (CLICKHOUSE_LAMBDA_CONTAINER_DIRECTORY 
    ${CMAKE_CURRENT_BINARY_DIR}/../lambda-container
)

foreach(file ${CLICKHOUSE_LAMBDA_CONTAINER_SUPPLEMENTS})
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/${file} ${CLICKHOUSE_LAMBDA_CONTAINER_DIRECTORY}/${file} COPYONLY)
endforeach()

add_custom_target(build_lambda_container
    DEPENDS clickhouse-lambda
    WORKING_DIRECTORY ${CLICKHOUSE_LAMBDA_CONTAINER_DIRECTORY}

    COMMAND ln -f ${CLICKHOUSE_LAMBDA_CONTAINER_DIRECTORY}/../clickhouse ${CLICKHOUSE_LAMBDA_CONTAINER_DIRECTORY}/clickhouse-lambda
    COMMAND docker build -t clickhouse-lambda .
)
