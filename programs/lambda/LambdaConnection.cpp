#include "LambdaConnection.h"

#include <Core/ExternalTable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

LambdaConnection::LambdaConnection(ContextPtr context)
    : LocalConnection(std::move(context), false, false, "")
{
    description = "clickhouse-lambda";
}


void LambdaConnection::sendQuery(
    const ConnectionTimeouts & timeouts,
    const String & query,
    const NameToNameMap & query_parameters,
    const String & query_id,
    UInt64 stage,
    const Settings * settings,
    const ClientInfo * client_info,
    bool with_pending_data,
    std::function<void(const Progress &)> process_progress_callback)
{
    deferred_parameters.send_query_parameters.emplace(SendQueryParameters{timeouts, query, query_parameters, query_id,
                                                      stage, settings, client_info, with_pending_data, std::move(process_progress_callback)});

    if (deferred_parameters.isReady())
        executeDeferredQuery();
}

void LambdaConnection::sendExternalTablesData(ExternalTablesData & external_tables_data)
{
    deferred_parameters.external_tables_data = std::move(external_tables_data);

    if (deferred_parameters.isReady())
        executeDeferredQuery();
}

void LambdaConnection::executeDeferredQuery()
{
    try
    {
        auto& params = * deferred_parameters.send_query_parameters;
        LocalConnection::sendQuery(std::get<0>(params), std::get<1>(params), std::get<2>(params), std::get<3>(params),
                                   std::get<4>(params), std::get<5>(params), std::get<6>(params), std::get<7>(params), std::get<8>(params));
                                   
        deferred_parameters.cleanup();
    }
    catch(...)
    {
        deferred_parameters.cleanup();
        throw;
    }
}

void LambdaConnection::createExternalTables()
{
    for (const auto & table_data : * deferred_parameters.external_tables_data)
    {
        auto temporary_id = StorageID::createEmpty();
        temporary_id.table_name = table_data->table_name;

        auto& data_pipe = *table_data->pipe;

        auto temporary_table = TemporaryTableHolder(query_context,
            ColumnsDescription{data_pipe.getHeader().getNamesAndTypesList()}, {});

        StoragePtr storage = temporary_table.getTable();
        query_context->addExternalTable(temporary_id.table_name, std::move(temporary_table));

        /// The data will be written directly to the table.
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        auto sink = storage->write(ASTPtr(), metadata_snapshot, query_context, /*async_insert=*/false);

        data_pipe.addTransform(std::move(sink));
        data_pipe.setSinks([&](const Block & header, Pipe::StreamType)
        {
            return std::make_shared<EmptySink>(header);
        });

        auto executor = data_pipe.execute();
        executor->execute(1, false);
    }
}

}
