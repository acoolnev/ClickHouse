#pragma once

#include <base/types.h>
#include <Core/ExternalTable.h>

namespace DB
{

class LambdaTable : public BaseExternalTable
{
public:
    LambdaTable(String name, const String& structure, String format, String data);

private:
    void initReadBuffer() final;

    String data;
};

}
