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

struct LambdaCommunicatorContext
{
    LambdaCommunicatorContext(size_t queue_size)
        : query_queue(queue_size)
        , response_queue(queue_size)
    {}

    using QueryQueue = ConcurrentBoundedQueue<LambdaQuery>;
    using ResponseQueue = ConcurrentBoundedQueue<std::pair<String, bool>>;

    QueryQueue query_queue;
    ResponseQueue response_queue;
};

class LambdaServerCommunicator
{
public:
    LambdaServerCommunicator(LambdaCommunicatorContext & context_)
        : context(context_)
    {}

    /// Returns a pair with the query result (or error message) and a bool indicating success.
    std::optional<std::pair<String, bool>> executeQuery(LambdaQuery && query)
    {
        bool pushed = context.query_queue.push(std::move(query));

        if (pushed)
        {
            if (std::pair<String, bool> response; context.response_queue.pop(response))
                return response;

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
    LambdaHandlerCommunicator(LambdaCommunicatorContext & context_)
        : context(context_)
    {}

    std::optional<LambdaQuery> popQuery()
    {
        LambdaQuery lambda_query;
        if (!context.query_queue.pop(lambda_query))
            return std::nullopt;

        return lambda_query;
    }

    bool pushResponse(String && response, bool success)
    {
        return context.response_queue.emplace(std::move(response), success);
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
