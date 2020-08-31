#include <Formats/FormatFactory.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/RabbitMQ/RabbitMQBlockInputStream.h>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

RabbitMQBlockInputStream::RabbitMQBlockInputStream(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    Context & context_,
    const Names & columns,
    bool ack_in_suffix_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
        , column_names(columns)
        , ack_in_suffix(ack_in_suffix_)
        , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
        , virtual_header(metadata_snapshot->getSampleBlockForColumns(
                    {"_exchange_name", "_channel_id", "_delivery_tag", "_redelivered", "_message_id"}, storage.getVirtuals(), storage.getStorageID()))
{
}


RabbitMQBlockInputStream::~RabbitMQBlockInputStream()
{
    if (!buffer)
        return;

    storage.pushReadBuffer(buffer);
}


Block RabbitMQBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}


void RabbitMQBlockInputStream::readPrefixImpl()
{
    auto timeout = std::chrono::milliseconds(context.getSettingsRef().rabbitmq_max_wait_ms.totalMilliseconds());
    buffer = storage.popReadBuffer(timeout);
}


bool RabbitMQBlockInputStream::needManualChannelUpdate()
{
    if (!buffer)
        return false;

    return !buffer->channelUsable() && buffer->channelAllowed() && storage.connectionRunning();
}


void RabbitMQBlockInputStream::updateChannel()
{
    if (!buffer)
        return;

    buffer->updateAckTracker();

    storage.updateChannel(buffer->getChannel());
    buffer->setupChannel();
}


Block RabbitMQBlockInputStream::readImpl()
{
    if (!buffer || finished)
        return Block();

    finished = true;

    MutableColumns result_columns = non_virtual_header.cloneEmptyColumns();
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(
            storage.getFormatName(), *buffer, non_virtual_header, context, 1);

    InputPort port(input_format->getPort().getHeader(), input_format.get());
    connect(input_format->getPort(), port);
    port.setNeeded();

    auto read_rabbitmq_message = [&]
    {
        size_t new_rows = 0;

        while (true)
        {
            auto status = input_format->prepare();

            switch (status)
            {
                case IProcessor::Status::Ready:
                    input_format->work();
                    break;

                case IProcessor::Status::Finished:
                    input_format->resetParser();
                    return new_rows;

                case IProcessor::Status::PortFull:
                {
                    auto chunk = port.pull();

                    auto chunk_rows = chunk.getNumRows();
                    new_rows += chunk_rows;

                    auto columns = chunk.detachColumns();

                    for (size_t i = 0, s = columns.size(); i < s; ++i)
                    {
                        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
                    }
                    break;
                }
                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::Wait:
                case IProcessor::Status::ExpandPipeline:
                    throw Exception("Source processor returned status " + IProcessor::statusToName(status), ErrorCodes::LOGICAL_ERROR);
            }
        }
    };

    size_t total_rows = 0;

    while (true)
    {
        if (buffer->eof())
            break;

        auto new_rows = read_rabbitmq_message();

        if (new_rows)
        {
            auto exchange_name = storage.getExchange();
            auto channel_id = buffer->getChannelID();
            auto delivery_tag = buffer->getDeliveryTag();
            auto redelivered = buffer->getRedelivered();
            auto message_id = buffer->getMessageID();

            buffer->updateAckTracker({delivery_tag, channel_id});

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(exchange_name);
                virtual_columns[1]->insert(channel_id);
                virtual_columns[2]->insert(delivery_tag);
                virtual_columns[3]->insert(redelivered);
                virtual_columns[4]->insert(message_id);
            }

            total_rows = total_rows + new_rows;
        }

        buffer->allowNext();

        if (buffer->queueEmpty() || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return Block();

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    return result_block;
}


void RabbitMQBlockInputStream::readSuffixImpl()
{
    if (ack_in_suffix)
        sendAck();
}

bool RabbitMQBlockInputStream::sendAck()
{
    if (!buffer || !buffer->channelUsable())
        return false;

    if (!buffer->ackMessages())
        return false;

    return true;
}

}
