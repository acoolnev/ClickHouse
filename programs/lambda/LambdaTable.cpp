#include "LambdaTable.h"

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

LambdaTable::LambdaTable(String name_, const String& structure_, String format_, String data_)
{
    /// Not used. Can be any name. Data is loaded from the corresponding parameter.
    /// TODO: check if it can be moved to ExternalTable class.
    file = name_;

    name = std::move(name_);
    format = std::move(format_);
    parseStructureFromStructureField(structure_);

    data = std::move(data_);
}

void LambdaTable::initReadBuffer()
{
    read_buffer = std::make_unique<ReadBufferFromMemory>(data);
}

}
