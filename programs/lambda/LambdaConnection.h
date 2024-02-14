#pragma once

#include <Client/LocalConnection.h>

#include <optional>
#include <tuple>


namespace DB
{

class LambdaConnection : public LocalConnection
{
public:
    LambdaConnection(ContextPtr context);

    const String & getDescription() const override { return description; }

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const NameToNameMap & query_parameters,
        const String & query_id/* = "" */,
        UInt64 stage/* = QueryProcessingStage::Complete */,
        const Settings * settings/* = nullptr */,
        const ClientInfo * client_info/* = nullptr */,
        bool with_pending_data/* = false */,
        std::function<void(const Progress &)> process_progress_callback) final;

    void sendExternalTablesData(ExternalTablesData & external_tables_data) final;

private:
    // Hide unused methods
    using LocalConnection::createConnection;

    void createExternalTables() final;

    void executeDeferredQuery();

    using SendQueryParameters = std::tuple<
        ConnectionTimeouts /* timeouts */,
        String /* query */,
        NameToNameMap /* query_parameters */,
        String /* query_id */,
        UInt64 /* stage */,
        const Settings * /* settings */,
        const ClientInfo * /* client_info */,
        bool /* with_pending_data */,
        std::function<void(const Progress &)> /* process_progress_callback */>;

    struct DeferredParameters
    {
        std::optional<SendQueryParameters> send_query_parameters;
        std::optional<ExternalTablesData> external_tables_data;

        bool isReady() const
        {
            return send_query_parameters && external_tables_data;
        }

        void cleanup()
        {
            send_query_parameters = std::nullopt;
            external_tables_data = std::nullopt;
        }
    };

    DeferredParameters deferred_parameters;
};

}
