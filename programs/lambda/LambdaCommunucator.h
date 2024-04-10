#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <optional>

namespace DB
{

struct LambdaQuery
{
    String query_text;
    String output_format;
    String input_format;
    String input_structure;
    String input_data;
};

struct LambdaResult
{
    String format;
    String data;
    String error;

    explicit LambdaResult() = default;

    LambdaResult(String format_, String data_)
        : format(std::move(format_)), data(std::move(data_))
    
    {}

    explicit LambdaResult(String error_)
        : error(std::move(error_))
    {}
};

struct LambdaCommunicatorContext
{
    explicit LambdaCommunicatorContext(size_t queue_size)
        : query_queue(queue_size)
        , response_queue(queue_size)
    {}

    using QueryQueue = ConcurrentBoundedQueue<LambdaQuery>;
    using ResponseQueue = ConcurrentBoundedQueue<LambdaResult>;

    QueryQueue query_queue;
    ResponseQueue response_queue;
};

class LambdaServerCommunicator
{
public:
    explicit LambdaServerCommunicator(LambdaCommunicatorContext & context_)
        : context(context_)
    {}

    /// Returns a pair with the query result (or error message) and a bool indicating success.
    std::optional<LambdaResult> executeQuery(LambdaQuery && query)
    {
        bool pushed = context.query_queue.push(std::move(query));

        if (pushed)
        {
            if (LambdaResult lambda_result; context.response_queue.pop(lambda_result))
                return lambda_result;

            return std::nullopt;
        }

        return std::nullopt;
    }

    void close()
    {
        context.query_queue.finish();
        context.response_queue.finish();
    }

private:
    LambdaCommunicatorContext& context;
};

class LambdaHandlerCommunicator
{
public:
    explicit LambdaHandlerCommunicator(LambdaCommunicatorContext & context_)
        : context(context_)
    {}

    std::optional<LambdaQuery> popQuery()
    {
        LambdaQuery lambda_query;
        if (!context.query_queue.pop(lambda_query))
            return std::nullopt;

        return lambda_query;
    }

    bool pushResponse(LambdaResult && lambda_result)
    {
        return context.response_queue.emplace(std::move(lambda_result));
    }

    void close()
    {
        context.query_queue.finish();
        context.response_queue.finish();
    }

private:
    LambdaCommunicatorContext & context;
};

}
