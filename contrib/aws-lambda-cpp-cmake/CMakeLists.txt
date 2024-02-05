set (DEFAULT_ENABLE_AWS_LAMBDA OFF)

if (ENABLE_LIBRARIES AND ENABLE_CURL)
    set (DEFAULT_ENABLE_AWS_LAMBDA ON)
endif()

option(ENABLE_AWS_LAMBDA "Enable aws-lambda-cpp" ${DEFAULT_ENABLE_AWS_LAMBDA})

if (NOT ENABLE_AWS_LAMBDA)
    message(STATUS "Not using aws-lambda-cpp")
    return()
endif()

set(AWS_LAMBDA_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-lambda-cpp")

add_library(_aws_lambda_runtime)

target_sources(_aws_lambda_runtime PRIVATE
    "${AWS_LAMBDA_SOURCE_DIR}/src/logging.cpp"
    "${AWS_LAMBDA_SOURCE_DIR}/src/runtime.cpp"
    "${AWS_LAMBDA_SOURCE_DIR}/src/backward.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/version.cpp"
)

target_include_directories(_aws_lambda_runtime SYSTEM PUBLIC "${AWS_LAMBDA_SOURCE_DIR}/include")

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/aws-lambda-runtime")

add_definitions(-Dno_backtrace)

target_link_libraries(_aws_lambda_runtime PUBLIC ch_contrib::curl)

add_library(ch_contrib::aws_lambda_runtime ALIAS _aws_lambda_runtime)
