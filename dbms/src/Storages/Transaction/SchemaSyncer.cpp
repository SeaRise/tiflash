#include <curl/curl.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseOrdinary.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>
#include <TiDB/TiDBService.h>
#include <common/JSON.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

class Curl final : public ext::singleton<Curl>
{
public:
    String getTiDBTableInfoJson(TableID table_id, Context & context);

private:
    Curl();
    ~Curl();

    friend class ext::singleton<Curl>;
};

Curl::Curl()
{
    CURLcode code = curl_global_init(CURL_GLOBAL_ALL);
    if (code != CURLE_OK)
        throw DB::Exception("CURL global init failed.", code);
}

Curl::~Curl() { curl_global_cleanup(); }

String Curl::getTiDBTableInfoJson(TableID table_id, Context & context)
{
    auto & tidb_service = context.getTiDBService();

    CURL * curl = curl_easy_init();

    curl_easy_setopt(curl,
        CURLOPT_URL,
        std::string("http://" + tidb_service.serviceIp() + ":" + tidb_service.statusPort() + "/db-table/" + toString(table_id)).c_str());

    auto writeFunc = [](void * buffer, size_t size, size_t nmemb, void * result) {
        auto str = reinterpret_cast<String *>(result);
        size_t real_size = size * nmemb;
        str->append((const char *)buffer, real_size);
        return real_size;
    };
    typedef size_t (*WriteFuncT)(void * buffer, size_t size, size_t nmemb, void * result);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, (WriteFuncT)writeFunc);

    String result;
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&result);

    CURLcode code = curl_easy_perform(curl);

    curl_easy_cleanup(curl);

    if (code != CURLE_OK)
        throw DB::Exception("Get TiDB schema through HTTP failed.", code);

    if (result.empty() || result[0] == '[')
    {
        throw DB::Exception("Table with ID = " + toString(table_id) + " does not exist in TiDB", ErrorCodes::LOGICAL_ERROR);
    }

    return result;
}

String getTiDBTableInfoJsonByCurl(TableID table_id, Context & context) { return Curl::instance().getTiDBTableInfoJson(table_id, context); }

using TableInfo = TiDB::TableInfo;
using ColumnInfo = TiDB::ColumnInfo;

String createDatabaseStmt(const TableInfo & table_info) { return "CREATE DATABASE " + table_info.db_name; }

void createDatabase(const TableInfo & table_info, Context & context)
{
    String stmt = createDatabaseStmt(table_info);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info.name, 0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = table_info.db_name;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
}

String createTableStmt(const TableInfo & table_info)
{
    NamesAndTypes columns;
    std::vector<String> pks;
    for (const auto & column : table_info.columns)
    {
        DataTypePtr type = getDataTypeByColumnInfo(column);
        columns.emplace_back(NameAndTypePair(column.name, type));

        if (column.hasPriKeyFlag())
        {
            pks.emplace_back(column.name);
        }
    }

    if (pks.size() != 1 || !table_info.pk_is_handle)
    {
        columns.emplace_back(NameAndTypePair(MutableSupport::tidb_pk_column_name, std::make_shared<DataTypeInt64>()));
        pks.clear();
        pks.emplace_back(MutableSupport::tidb_pk_column_name);
    }

    String stmt;
    WriteBufferFromString stmt_buf(stmt);
    writeString("CREATE TABLE ", stmt_buf);
    writeBackQuotedString(table_info.db_name, stmt_buf);
    writeString(".", stmt_buf);
    writeBackQuotedString(table_info.name, stmt_buf);
    writeString("(", stmt_buf);
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (i > 0)
            writeString(", ", stmt_buf);
        writeBackQuotedString(columns[i].name, stmt_buf);
        writeString(" ", stmt_buf);
        writeString(columns[i].type->getName(), stmt_buf);
    }
    writeString(") Engine = TxnMergeTree((", stmt_buf);
    for (size_t i = 0; i < pks.size(); i++)
    {
        if (i > 0)
            writeString(", ", stmt_buf);
        writeBackQuotedString(pks[i], stmt_buf);
    }
    writeString("), 8192, '", stmt_buf);
    writeString(table_info.serialize(true), stmt_buf);
    writeString("')", stmt_buf);

    return stmt;
}

void createTable(const TableInfo & table_info, Context & context)
{
    String stmt = createTableStmt(table_info);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, stmt.data(), stmt.data() + stmt.size(), "from syncSchema " + table_info.name, 0);

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = true;
    ast_create_query.database = table_info.db_name;

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    interpreter.setForceRestoreData(false);
    interpreter.execute();
}

AlterCommands detectSchemaChanges(const TableInfo & table_info, const TableInfo & orig_table_info)
{
    AlterCommands alter_commands;

    /// Detect new columns.
    // TODO: Detect rename or type-changed columns.
    for (const auto & column_info : table_info.columns)
    {
        const auto & orig_column_info = std::find_if(orig_table_info.columns.begin(),
            orig_table_info.columns.end(),
            [&](const ColumnInfo & orig_column_info_) { return orig_column_info_.id == column_info.id; });

        AlterCommand command;
        if (orig_column_info == orig_table_info.columns.end())
        {
            // New column.
            command.type = AlterCommand::ADD_COLUMN;
            command.column_name = column_info.name;
            command.data_type = getDataTypeByColumnInfo(column_info);
            // TODO: support default value.
            // TODO: support after column.
        }
        else
        {
            // Column unchanged.
            continue;
        }

        alter_commands.emplace_back(std::move(command));
    }

    /// Detect dropped columns.
    for (const auto & orig_column_info : orig_table_info.columns)
    {
        const auto & column_info = std::find_if(table_info.columns.begin(), table_info.columns.end(), [&](const ColumnInfo & column_info_) {
            return column_info_.id == orig_column_info.id;
        });

        AlterCommand command;
        if (column_info == table_info.columns.end())
        {
            // Dropped column.
            command.type = AlterCommand::DROP_COLUMN;
            command.column_name = orig_column_info.name;
        }
        else
        {
            // Column unchanged.
            continue;
        }

        alter_commands.emplace_back(std::move(command));
    }

    return alter_commands;
}

JsonSchemaSyncer::JsonSchemaSyncer() : log(&Logger::get("SchemaSyncer")) {}

void JsonSchemaSyncer::syncSchema(TableID table_id, Context & context, bool force)
{
    // Do nothing if table already exists unless forced,
    // so that we don't grab schema from TiDB, which is costly, on every syncSchema call.
    auto & tmt_context = context.getTMTContext();
    if (!force && tmt_context.storages.get(table_id))
        return;

    /// Get table schema json from TiDB/TiKV.
    String table_info_json = getSchemaJson(table_id, context);

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Table " << table_id << " info json: " << table_info_json);

    TableInfo table_info(table_info_json, false);

    auto storage = tmt_context.storages.get(table_id);

    if (storage == nullptr)
    {
        /// Table not existing, create it.
        if (!context.isDatabaseExist(table_info.db_name))
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Creating database " << table_info.db_name);
            createDatabase(table_info, context);
        }

        if (!context.isTableExist(table_info.db_name, table_info.name))
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Creating table " << table_info.name);
            createTable(table_info, context);
            context.getTMTContext().storages.put(context.getTable(table_info.db_name, table_info.name));
        }

        /// Mangle for partition table.
        bool is_partition_table = table_info.manglePartitionTableIfNeeded(table_id);
        if (is_partition_table && !context.isTableExist(table_info.db_name, table_info.name))
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Re-creating table after mangling partition table " << table_info.name);
            createTable(table_info, context);
            context.getTMTContext().storages.put(context.getTable(table_info.db_name, table_info.name));
        }
        return;
    }

    /// Table existing, detect schema changes and apply.
    auto merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(storage);
    const TableInfo & orig_table_info = merge_tree->getTableInfo();
    AlterCommands alter_commands = detectSchemaChanges(table_info, orig_table_info);

    std::stringstream ss;
    ss << "Detected schema changes: ";
    for (const auto & command : alter_commands)
    {
        // TODO: Other command types.
        if (command.type == AlterCommand::ADD_COLUMN)
            ss << "ADD COLUMN " << command.column_name << " " << command.data_type->getName() << ", ";
        else if (command.type == AlterCommand::DROP_COLUMN)
            ss << "DROP COLUMN " << command.column_name << ", ";
    }

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": " << ss.str());

    {
        // Change internal TableInfo in TMT first.
        // TODO: Ideally this should be done within alter function, however we are limited by the narrow alter interface, thus not truly atomic.
        auto table_hard_lock = storage->lockStructureForAlter(__PRETTY_FUNCTION__);
        merge_tree->setTableInfo(table_info);
    }

    // Call storage alter to apply schema changes.
    storage->alter(alter_commands, table_info.db_name, table_info.name, context);

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Schema changes apply done.");

    // TODO: Apply schema changes to partition tables.
}

String HttpJsonSchemaSyncer::getSchemaJson(TableID table_id, Context & context) { return getTiDBTableInfoJsonByCurl(table_id, context); }

} // namespace DB
