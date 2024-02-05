// Implements the interface defined in include/aws/lambda-runtime/version.h.
// The original CMake file from the aws-lambda-cpp module autogenerates
// version.cpp from src/version.cpp.in. However, since ClickHouse uses its own
// CMake files, version.cpp has to be implemented explicitly.

#pragma once

namespace aws {
namespace lambda_runtime {

// The version should be updated after the aws-lambda-cpp module is updated.
const char* AWS_LAMBDA_RUNTIME_VERSION = "0.0.0";
const unsigned int AWS_LAMBDA_RUNTIME_VERSION_MAJOR = 0;
const unsigned int AWS_LAMBDA_RUNTIME_VERSION_MINOR = 0;
const unsigned int AWS_LAMBDA_RUNTIME_VERSION_PATCH = 0;

unsigned int get_version_major()
{
    return AWS_LAMBDA_RUNTIME_VERSION_MAJOR;
}

unsigned int get_version_minor()
{
    return AWS_LAMBDA_RUNTIME_VERSION_MINOR;
}

unsigned int get_version_patch()
{
    return AWS_LAMBDA_RUNTIME_VERSION_PATCH;
}

char const* get_version()
{
    return AWS_LAMBDA_RUNTIME_VERSION;
}

} // namespace lambda_runtime
} // namespace aws
