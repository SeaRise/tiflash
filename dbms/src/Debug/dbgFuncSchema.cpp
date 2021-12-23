#include <Common/FmtUtils.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseTiFlash.h>
#include <Debug/dbgFuncSchema.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/SchemaSyncService.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

void dbgFuncEnableSchemaSyncService(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: enable (true/false)", ErrorCodes::BAD_ARGUMENTS);

    bool enable = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value) == "true";

    if (enable)
    {
        if (!context.getSchemaSyncService())
            context.initializeSchemaSyncService();
    }
    else
    {
        if (context.getSchemaSyncService())
            context.getSchemaSyncService().reset();
    }

    output(fmt::format("schema sync service {}", (enable ? "enabled" : "disabled")));
}

void dbgFuncRefreshSchemas(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->syncSchemas(context);

    output("schemas refreshed");
}

// Trigger gc on all databases / tables.
// Usage:
//   ./storage-client.sh "DBGInvoke gc_schemas([gc_safe_point])"
void dbgFuncGcSchemas(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    auto & service = context.getSchemaSyncService();
    Timestamp gc_safe_point = 0;
    if (args.size() == 0)
        gc_safe_point = PDClientHelper::getGCSafePointWithRetry(context.getTMTContext().getPDClient());
    else
        gc_safe_point = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    service->gc(gc_safe_point);

    output("schemas gc done");
}

void dbgFuncResetSchemas(Context & context, const ASTs &, DBGInvoker::Printer output)
{
    TMTContext & tmt = context.getTMTContext();
    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->reset();

    output("reset schemas");
}

void dbgFuncIsTombstone(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() < 1 || args.size() > 2)
        throw Exception("Args not matched, should be: database-name[, table-name]", ErrorCodes::BAD_ARGUMENTS);

    const String & database_name = typeid_cast<const ASTIdentifier &>(*args[0]).name;
    FmtBuffer fmt_buf;
    if (args.size() == 1)
    {
        auto db = context.getDatabase(database_name);
        auto tiflash_db = std::dynamic_pointer_cast<DatabaseTiFlash>(db);
        if (!tiflash_db)
            throw Exception(database_name + " is not DatabaseTiFlash", ErrorCodes::BAD_ARGUMENTS);

        fmt_buf.append((tiflash_db->isTombstone() ? "true" : "false"));
    }
    else if (args.size() == 2)
    {
        const String & table_name = typeid_cast<const ASTIdentifier &>(*args[1]).name;
        auto storage = context.getTable(database_name, table_name);
        auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
        if (!managed_storage)
            throw Exception(database_name + "." + table_name + " is not ManageableStorage", ErrorCodes::BAD_ARGUMENTS);

        fmt_buf.append((managed_storage->isTombstone() ? "true" : "false"));
    }
    output(fmt_buf.toString());
}

} // namespace DB
