#pragma once

#include "LambdaCommunucator.h"

#include <Client/ClientBase.h>
#include <Client/LocalConnection.h>

#include <Common/InterruptListener.h>
#include <Common/StatusFile.h>
#include <Loggers/Loggers.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <filesystem>
#include <memory>
#include <optional>


namespace DB
{

class LambdaServer : public ClientBase, public Loggers
{
public:
    LambdaServer(LambdaHandlerCommunicator & lambda_communicator);
    ~LambdaServer() override;

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & /*args*/) override;

protected:
    void connect() override;

    void processError(const String & query) const override;

    String getName() const override { return "lambda"; }

    void printHelpMessage(const OptionsDescription & options_description) override;

    void addOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description, const CommandLineOptions & options,
                        const std::vector<Arguments> &, const std::vector<Arguments> &) override;

    void processConfig() override;
    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &, std::vector<Arguments> &) override;


    void updateLoggerLevel(const String & logs_level) override;

    void initOutputFormat(const Block & block, ASTPtr parsed_query) final;

private:
    void runQueryLoop();

    void tryInitPath();
    void setupUsers();
    void cleanup();

    void applyCmdOptions(ContextMutablePtr context);
    void applyCmdSettings(ContextMutablePtr context);

    std::optional<StatusFile> status;
    std::optional<std::filesystem::path> temporary_directory_to_delete;

    String query_response;

    LambdaHandlerCommunicator & lambda_communicator;
};

}
